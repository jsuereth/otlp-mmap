use crate::sdk_mmap::data;
use memmap2::MmapMut;
use prost::Message;
use std::fs::OpenOptions;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct OtlpMmapExporter {
    mmap: MmapMut,
    events_offset: usize,
    spans_offset: usize,
    measurements_offset: usize,
    dictionary_offset: usize,
}

const FILE_SIZE: u64 = 64 * 1024 * 1024; // 64 MB default

// Header Offsets
const OFFSET_VERSION: usize = 0;
const OFFSET_EVENTS: usize = 8;
const OFFSET_SPANS: usize = 16;
const OFFSET_MEASUREMENTS: usize = 24;
const OFFSET_DICTIONARY: usize = 32;
const OFFSET_START_TIME: usize = 40;

// RingBuffer Header
const RB_OFFSET_NUM_BUFFERS: usize = 0;
const RB_OFFSET_BUFFER_SIZE: usize = 8;
const RB_OFFSET_READ_POS: usize = 16;
const RB_OFFSET_WRITE_POS: usize = 24;
const RB_HEADER_SIZE: usize = 32;

// Defaults
const DEFAULT_NUM_BUFFERS: u64 = 1024;
const DEFAULT_BUFFER_SIZE: u64 = 512; // bytes per chunk

impl OtlpMmapExporter {
    pub fn new(path: &str) -> anyhow::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let metadata = file.metadata()?;
        if metadata.len() < FILE_SIZE {
            file.set_len(FILE_SIZE)?;
        }

        let mut mmap = unsafe { MmapMut::map_mut(&file)? };

        // Initialize header if needed
        let version = u64::from_le_bytes(mmap[OFFSET_VERSION..OFFSET_VERSION + 8].try_into()?);
        let mut events_offset = u64::from_le_bytes(mmap[OFFSET_EVENTS..OFFSET_EVENTS + 8].try_into()?) as usize;
        let mut spans_offset = u64::from_le_bytes(mmap[OFFSET_SPANS..OFFSET_SPANS + 8].try_into()?) as usize;
        let mut measurements_offset = u64::from_le_bytes(mmap[OFFSET_MEASUREMENTS..OFFSET_MEASUREMENTS + 8].try_into()?) as usize;
        let mut dictionary_offset = u64::from_le_bytes(mmap[OFFSET_DICTIONARY..OFFSET_DICTIONARY + 8].try_into()?) as usize;

        if version == 0 {
            // Initialize
            let mut offset = 64; // Header size
            
            // Version
            mmap[OFFSET_VERSION..OFFSET_VERSION + 8].copy_from_slice(&1u64.to_le_bytes());

            // Start Time
            let start_time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as u64;
            mmap[OFFSET_START_TIME..OFFSET_START_TIME + 8].copy_from_slice(&start_time.to_le_bytes());

            // Events RingBuffer
            events_offset = offset;
            mmap[OFFSET_EVENTS..OFFSET_EVENTS + 8].copy_from_slice(&(events_offset as u64).to_le_bytes());
            Self::init_ring_buffer(&mut mmap, events_offset, DEFAULT_NUM_BUFFERS, DEFAULT_BUFFER_SIZE);
            offset += Self::ring_buffer_size(DEFAULT_NUM_BUFFERS, DEFAULT_BUFFER_SIZE);

            // Spans RingBuffer
            spans_offset = offset;
            mmap[OFFSET_SPANS..OFFSET_SPANS + 8].copy_from_slice(&(spans_offset as u64).to_le_bytes());
            Self::init_ring_buffer(&mut mmap, spans_offset, DEFAULT_NUM_BUFFERS, DEFAULT_BUFFER_SIZE);
            offset += Self::ring_buffer_size(DEFAULT_NUM_BUFFERS, DEFAULT_BUFFER_SIZE);

            // Measurements RingBuffer
            measurements_offset = offset;
            mmap[OFFSET_MEASUREMENTS..OFFSET_MEASUREMENTS + 8].copy_from_slice(&(measurements_offset as u64).to_le_bytes());
            Self::init_ring_buffer(&mut mmap, measurements_offset, DEFAULT_NUM_BUFFERS, DEFAULT_BUFFER_SIZE);
            offset += Self::ring_buffer_size(DEFAULT_NUM_BUFFERS, DEFAULT_BUFFER_SIZE);

            // Dictionary
            dictionary_offset = offset;
            mmap[OFFSET_DICTIONARY..OFFSET_DICTIONARY + 8].copy_from_slice(&(dictionary_offset as u64).to_le_bytes());
            
            // Initialize dictionary index/offset
            mmap[dictionary_offset..dictionary_offset+8].copy_from_slice(&8u64.to_le_bytes()); // Start after the size field
        }

        Ok(Self {
            mmap,
            events_offset,
            spans_offset,
            measurements_offset,
            dictionary_offset,
        })
    }

    fn ring_buffer_size(num_buffers: u64, buffer_size: u64) -> usize {
        // Header + Availability + Buffers
        RB_HEADER_SIZE + (4 * num_buffers as usize) + (num_buffers as usize * buffer_size as usize)
    }

    fn init_ring_buffer(mmap: &mut MmapMut, offset: usize, num_buffers: u64, buffer_size: u64) {
        // Header
        mmap[offset + RB_OFFSET_NUM_BUFFERS..offset + RB_OFFSET_NUM_BUFFERS + 8]
            .copy_from_slice(&num_buffers.to_le_bytes());
        mmap[offset + RB_OFFSET_BUFFER_SIZE..offset + RB_OFFSET_BUFFER_SIZE + 8]
            .copy_from_slice(&buffer_size.to_le_bytes());
        // Read/Write positions initialized to -1 (u64::MAX)
        mmap[offset + RB_OFFSET_READ_POS..offset + RB_OFFSET_READ_POS + 8]
            .copy_from_slice(&u64::MAX.to_le_bytes());
        mmap[offset + RB_OFFSET_WRITE_POS..offset + RB_OFFSET_WRITE_POS + 8]
            .copy_from_slice(&u64::MAX.to_le_bytes());

        // Availability Array
        let avail_offset = offset + RB_HEADER_SIZE;
        let avail_size = 4 * num_buffers as usize;
        let avail_slice = &mut mmap[avail_offset..avail_offset + avail_size];
        // Fill with -1 (0xFFFFFFFF)
        for chunk in avail_slice.chunks_exact_mut(4) {
            chunk.copy_from_slice(&u32::MAX.to_le_bytes());
        }
    }

    // Helper for writing to a ring buffer
    fn write_to_ring_buffer<T: Message>(&mut self, rb_offset: usize, msg: &T) -> anyhow::Result<()> {
        let num_buffers = u64::from_le_bytes(self.mmap[rb_offset + RB_OFFSET_NUM_BUFFERS..rb_offset + RB_OFFSET_NUM_BUFFERS + 8].try_into()?);
        let buffer_size = u64::from_le_bytes(self.mmap[rb_offset + RB_OFFSET_BUFFER_SIZE..rb_offset + RB_OFFSET_BUFFER_SIZE + 8].try_into()?);
        
        let write_pos_ptr = unsafe { self.mmap.as_ptr().add(rb_offset + RB_OFFSET_WRITE_POS) as *const AtomicU64 };
        let read_pos_ptr = unsafe { self.mmap.as_ptr().add(rb_offset + RB_OFFSET_READ_POS) as *const AtomicU64 };
        
        // Safety: Mmap is pinned and we are using atomics.
        let write_pos = unsafe { &*write_pos_ptr };
        let read_pos = unsafe { &*read_pos_ptr };

        let encoded_len = msg.encoded_len();
        if encoded_len as u64 > buffer_size {
             return Err(anyhow::anyhow!("Message too large for buffer"));
        }

        let mut current_idx = write_pos.load(Ordering::Acquire);
        loop {
            // Check capacity
            let reader_pos = read_pos.load(Ordering::Acquire);
            
            if current_idx.wrapping_sub(reader_pos) >= num_buffers {
                 std::thread::yield_now();
                 current_idx = write_pos.load(Ordering::Acquire);
                 continue;
            }

            let next_idx = current_idx.wrapping_add(1);
            if write_pos.compare_exchange_weak(current_idx, next_idx, Ordering::Release, Ordering::Relaxed).is_ok() {
                let target_idx = next_idx; 
                let ring_idx = (target_idx % num_buffers) as usize;
                
                let avail_offset = rb_offset + RB_HEADER_SIZE;
                let chunk_offset = avail_offset + (4 * num_buffers as usize) + (ring_idx * buffer_size as usize);
                
                let chunk_slice = &mut self.mmap[chunk_offset..chunk_offset + buffer_size as usize];
                // Use encode_length_delimited to match mmap-collector's reader
                let mut buf = &mut chunk_slice[..];
                msg.encode_length_delimited(&mut buf)?;

                // Mark Available
                let avail_ptr = unsafe { self.mmap.as_ptr().add(avail_offset + ring_idx * 4) as *const AtomicI32 };
                let avail = unsafe { &*avail_ptr };
                
                let shift = num_buffers.trailing_zeros();
                let flag = (target_idx >> shift) as i32;
                avail.store(flag, Ordering::Release);
                
                break;
            }
            // CAS failed, retry
             current_idx = write_pos.load(Ordering::Acquire);
        }

        Ok(())
    }

    pub fn write_dictionary_entry<T: Message>(&mut self, msg: &T) -> anyhow::Result<usize> {
        let dict_start = self.dictionary_offset;
        let write_offset_ptr = unsafe { self.mmap.as_ptr().add(dict_start) as *const AtomicU64 };
        let write_offset_atomic = unsafe { &*write_offset_ptr };
        
        let current_rel_pos = write_offset_atomic.load(Ordering::Acquire);
        let encoded_len = msg.encoded_len();
        let delimiter_len = prost::length_delimiter_len(encoded_len);
        let total_len = delimiter_len + encoded_len;
        
        if (dict_start as u64 + current_rel_pos + total_len as u64) > FILE_SIZE {
             return Err(anyhow::anyhow!("Dictionary full"));
        }
        
        let abs_pos = dict_start + current_rel_pos as usize;
        let slice = &mut self.mmap[abs_pos..abs_pos+total_len];
        let mut buf = &mut slice[..];
        msg.encode_length_delimited(&mut buf)?;
        
        write_offset_atomic.store(current_rel_pos + total_len as u64, Ordering::Release);
        
        Ok(abs_pos)
    }

    fn write_raw_bytes(&mut self, bytes: &[u8]) -> anyhow::Result<usize> {
        let dict_start = self.dictionary_offset;
        let write_offset_ptr = unsafe { self.mmap.as_ptr().add(dict_start) as *const AtomicU64 };
        let write_offset_atomic = unsafe { &*write_offset_ptr };
        
        let current_rel_pos = write_offset_atomic.load(Ordering::Acquire);
        let len = bytes.len();
        let delimiter_len = prost::length_delimiter_len(len);
        let total_len = delimiter_len + len;
        
        if (dict_start as u64 + current_rel_pos + total_len as u64) > FILE_SIZE {
             return Err(anyhow::anyhow!("Dictionary full"));
        }
        
        let abs_pos = dict_start + current_rel_pos as usize;
        let slice = &mut self.mmap[abs_pos..abs_pos+total_len];
        let mut buf = &mut slice[..];
        
        prost::encoding::encode_varint(len as u64, &mut buf);
        buf.copy_from_slice(bytes);
        
        write_offset_atomic.store(current_rel_pos + total_len as u64, Ordering::Release);
        
        Ok(abs_pos)
    }

    // Public methods for the exporter
    
    pub fn record_string(&mut self, s: &str) -> anyhow::Result<usize> {
        self.write_raw_bytes(s.as_bytes())
    }

    fn intern_attributes(&mut self, attributes: Vec<(String, data::AnyValue)>) -> anyhow::Result<Vec<data::KeyValueRef>> {
        let mut kvs = Vec::with_capacity(attributes.len());
        for (k, v) in attributes {
            let key_ref = self.record_string(&k)? as i64;
            kvs.push(data::KeyValueRef {
                key_ref,
                value: Some(v),
            });
        }
        Ok(kvs)
    }

    pub fn create_resource(&mut self, attributes: Vec<(String, data::AnyValue)>, _schema_url: Option<String>) -> anyhow::Result<usize> {
         let kvs = self.intern_attributes(attributes)?;
         let res = data::Resource {
             attributes: kvs,
             dropped_attributes_count: 0,
         };
         self.write_dictionary_entry(&res)
    }
    
    pub fn create_instrumentation_scope(&mut self, resource_ref: usize, name: String, version: Option<String>, attributes: Vec<(String, data::AnyValue)>) -> anyhow::Result<usize> {
        let kvs = self.intern_attributes(attributes)?;
        let name_ref = self.record_string(&name)? as i64;
        let version_ref = if let Some(v) = version {
            self.record_string(&v)? as i64
        } else {
            0
        };
        
        let scope = data::InstrumentationScope {
            name_ref,
            version_ref,
            attributes: kvs,
            dropped_attributes_count: 0,
            resource_ref: resource_ref as i64,
        };
        self.write_dictionary_entry(&scope)
    }

    pub fn create_metric_stream(&mut self, scope_ref: usize, name: String, description: String, unit: String, aggregation: Option<data::metric_ref::Aggregation>) -> anyhow::Result<usize> {
         let metric = data::MetricRef {
             name,
             description,
             unit,
             instrumentation_scope_ref: scope_ref as i64,
             aggregation,
         };
         self.write_dictionary_entry(&metric)
    }

    pub fn record_measurement(&mut self, metric_ref: usize, attributes: Vec<(String, data::AnyValue)>, time: u64, value: data::measurement::Value, span_context: Option<data::SpanContext>) -> anyhow::Result<()> {
        let kvs = self.intern_attributes(attributes)?;
        let m = data::Measurement {
            metric_ref: metric_ref as i64,
            attributes: kvs,
            time_unix_nano: time,
            span_context,
            value: Some(value),
        };
        self.write_to_ring_buffer(self.measurements_offset, &m)
    }

    pub fn record_event(&mut self, scope_ref: usize, span_context: Option<data::SpanContext>, event_name_ref: usize, time: u64, attributes: Vec<(String, data::AnyValue)>) -> anyhow::Result<()> {
        let kvs = self.intern_attributes(attributes)?;
        let e = data::Event {
            scope_ref: scope_ref as i64,
            time_unix_nano: time,
            event_name_ref: event_name_ref as i64,
            span_context,
            attributes: kvs,
            severity_number: 0,
            severity_text: "".to_string(),
            body: None
        };
        self.write_to_ring_buffer(self.events_offset, &e)
    }
    
    pub fn record_span_event(&mut self, scope_ref: usize, trace_id: Vec<u8>, span_id: Vec<u8>, event: data::span_event::Event) -> anyhow::Result<()> {
        let s = data::SpanEvent {
            scope_ref: scope_ref as i64,
            trace_id,
            span_id,
            event: Some(event),
        };
        self.write_to_ring_buffer(self.spans_offset, &s)
    }
    
    pub fn intern_attributes_public(&mut self, attributes: Vec<(String, data::AnyValue)>) -> anyhow::Result<Vec<data::KeyValueRef>> {
        self.intern_attributes(attributes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn test_layout() -> anyhow::Result<()> {
        let tmp_dir = tempfile::tempdir()?;
        let file_path = tmp_dir.path().join("test.mmap");
        let path_str = file_path.to_str().unwrap();

        let mut exporter = OtlpMmapExporter::new(path_str)?;

        // Verify Header Offsets
        // Read raw file
        let mut file = std::fs::File::open(&file_path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        // Version
        let version = u64::from_le_bytes(buffer[0..8].try_into()?);
        assert_eq!(version, 1);

        // Offsets
        let events_off = u64::from_le_bytes(buffer[8..16].try_into()?);
        let spans_off = u64::from_le_bytes(buffer[16..24].try_into()?);
        let measurements_off = u64::from_le_bytes(buffer[24..32].try_into()?);
        let dict_off = u64::from_le_bytes(buffer[32..40].try_into()?);

        assert_eq!(events_off, 64);
        
        let rb_size = 32 + (4 * DEFAULT_NUM_BUFFERS) + (DEFAULT_NUM_BUFFERS * DEFAULT_BUFFER_SIZE);
        assert_eq!(spans_off, 64 + rb_size);
        assert_eq!(measurements_off, spans_off + rb_size);
        assert_eq!(dict_off, measurements_off + rb_size);

        // Test writing a string
        let _idx = exporter.record_string("hello")?;
        
        // Re-read file
        let mut file = std::fs::File::open(&file_path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        
        let dict_start = dict_off as usize;
        // First 8 bytes is write pos
        // let write_pos = u64::from_le_bytes(buffer[dict_start..dict_start+8].try_into()?);
        
        // Let's verify content at dict_start + 8
        let entry_start = dict_start + 8;
        // Decode the message to verify
        let mut msg_bytes = &buffer[entry_start..];
        let len = prost::encoding::decode_varint(&mut msg_bytes)?;
        let s_bytes = &msg_bytes[..len as usize];
        let s = std::str::from_utf8(s_bytes)?;
        assert_eq!(s, "hello");

        Ok(())
    }
}