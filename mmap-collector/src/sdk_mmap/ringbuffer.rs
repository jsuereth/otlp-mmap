//! Multi-Producer, single-consumer ring buffer.
//!
//! A RingBuffer is the heart of the OTLP-MMAP implementation.
//! This package provides everything we need to read, efficiently, from a RingBuffer.
//!
//! A RingBuffer is structured as follows:
//! | Header | Availability Array | Buffer1 | ... | BufferN |
//!
//! More than one process is expected to mount a file containing the ring buffer, and
//! communication between processes MUST be done via atomic memory operations (e.g. CAS).
//!
//! This ringbuffer is designed to allow multi-producer, single-consumer.
//! The header contains information about the position of the single reader, in
//! addition to the *maximum taken* buffers for writing. When a producer is finished
//! writing to a buffer, it will update the availability array with a given flag.
//! Reader MUST check the availability array to ensure a buffer is complete before reading.

use std::{
    marker::PhantomData,
    ops::Deref,
    sync::atomic::{AtomicI32, AtomicI64, Ordering},
    time::Duration,
};

#[cfg(test)]
use std::ops::DerefMut;

use memmap2::MmapMut;
use tokio::sync::Mutex;

use crate::sdk_mmap::Error;

/// Async access to RingBuffer inputs.
///
/// Thread-safe across threads.
pub struct RingBufferReader<T> {
    input: Mutex<RawRingBuffer>,
    phantom: PhantomData<T>,
}

impl<T> RingBufferReader<T>
where
    T: prost::Message + std::default::Default + 'static,
{
    /// Constructs a new ring buffer on an mmap at the offset.
    pub fn new(data: MmapMut, offset: usize) -> RingBufferReader<T> {
        RingBufferReader {
            input: Mutex::new(RawRingBuffer::new(data, offset)),
            phantom: PhantomData,
        }
    }

    /// Reads the next input on this ringbuffer.
    /// Note: This will lock the ringbuffer from access for the duration.
    pub async fn next(&self) -> Result<T, Error> {
        // We need to make sure, conceptually, we're only reading from one thread.
        let input = self.input.lock().await;
        for _ in 0..10 {
            if let Some(result) = input.try_read::<T>()? {
                // println!("Read {} event on fast path", std::any::type_name::<T>());
                return Ok(result);
            } else {
                tokio::task::yield_now().await;
            }
        }
        // Sleep spin, exponentially slower.
        let mut d = Duration::from_millis(1);
        loop {
            if let Some(result) = input.try_read::<T>()? {
                // println!("Read {} event on slow path", std::any::type_name::<T>());
                return Ok(result);
            } else {
                tokio::time::sleep(d).await;
            }
            // TODO - Cap max wait time configuration.
            if d.as_secs() < 1 {
                d *= 2;
            }
        }
    }
}

/// A mmap ringbuffer implementation.
///
/// Note: This is currently designed to only allow ONE consumer
///       but multiple prodcuers.
struct RawRingBuffer {
    /// The mmap data
    data: MmapMut,
    /// The offset into the mmap data where the ringbuffer starts.
    offset: usize,
    /// Efficient mechanism to convert a message index into
    /// an availability flag.  Effectively - size.ilog2()
    shift: u32,
}

impl RawRingBuffer {
    /// Constructs a new ring buffer on an mmap at the offset.
    fn new(data: MmapMut, offset: usize) -> RawRingBuffer {
        let hdr = unsafe { &*(data.as_ref().as_ptr().add(offset) as *const RawRingBufferHeader) };
        RawRingBuffer {
            data,
            offset,
            shift: (hdr.num_buffers as u32).ilog2(),
        }
    }

    /// Constructs a new ring buffer, but sets the header values for it as well.
    #[cfg(test)]
    fn new_for_write(
        mut data: MmapMut,
        offset: usize,
        buffer_size: usize,
        num_buffers: usize,
    ) -> RawRingBuffer {
        // TODO - Validate memory bounds on MmapMut.
        unsafe {
            // Set header for RingBuffer
            let num_buffers_ptr = data.as_mut_ptr().add(offset) as *mut i64;
            *num_buffers_ptr = num_buffers as i64;
            let buffer_size_ptr = data.as_mut_ptr().add(offset + 8) as *mut i64;
            *buffer_size_ptr = buffer_size as i64;
            let read_posiiton_ptr = data.as_mut_ptr().add(offset + 16) as *mut i64;
            *read_posiiton_ptr = -1;
            let write_posiiton_ptr = data.as_mut_ptr().add(offset + 24) as *mut i64;
            *write_posiiton_ptr = -1;
            // Set availability array to -1.
            for i in 0..num_buffers {
                // Offset is overall offset + HEADER + offset into availability array.
                let av_offset = offset + 32 + i * 4;
                let av_ptr = data.as_mut_ptr().add(av_offset) as *mut i32;
                *av_ptr = -1;
            }
        }
        Self::new(data, offset)
    }

    fn try_read<T: prost::Message + std::default::Default>(&self) -> Result<Option<T>, Error> {
        if let Some(idx) = self.try_obtain_read_idx() {
            let result = Ok(Some(T::decode_length_delimited(self.entry(idx).deref())?));
            // Bump reader position to mark we've read this value.
            self.header().reader_index.store(idx, Ordering::Release);
            result
        } else {
            Ok(None)
        }
    }

    #[cfg(test)]
    fn try_write<T: prost::Message + std::fmt::Debug>(&mut self, msg: &T) -> Result<bool, Error> {
        if let Some(idx) = self.try_obtain_write_idx() {
            println!("Writing index {idx}: {msg:?}");
            msg.encode_length_delimited(&mut self.entry_mut(idx).deref_mut())?;
            self.set_read_available(idx);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Checks to see if we can read the next available buffer.
    ///
    /// Note: This will perform TWO atomic operations, one to get current position
    ///       an a second to confirm buffer availability.
    fn try_obtain_read_idx(&self) -> Option<i64> {
        let next = self.header().reader_index.load(Ordering::Acquire) + 1;
        if self.is_read_available(next) {
            Some(next)
        } else {
            None
        }
    }

    /// Attempts to obtain a write index or None, if buffer is full.
    #[cfg(test)]
    fn try_obtain_write_idx(&self) -> Option<i64> {
        let current = self.header().writer_index.load(Ordering::Acquire);
        let reader = self.header().reader_index.load(Ordering::Acquire);
        let num_buffers = self.header().num_buffers;
        let has_capacity = (current + 1 - num_buffers) < reader;
        if has_capacity
            && self
                .header()
                .writer_index
                .compare_exchange(current, current + 1, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
        {
            Some(current + 1)
        } else {
            None
        }
    }

    /// The ring buffer header (with atomic access).
    fn header(&self) -> &RawRingBufferHeader {
        unsafe { &*(self.data.as_ref().as_ptr().add(self.offset) as *const RawRingBufferHeader) }
    }
    /// The availability array for ring buffer entries.
    fn availability_array(&self) -> &[AtomicI32] {
        unsafe {
            let start_ptr = self
                .data
                .as_ref()
                .as_ptr()
                .add(self.availability_array_offset())
                .cast::<AtomicI32>();
            std::slice::from_raw_parts(start_ptr, self.header().num_buffers as usize)
        }
    }
    /// The number of bytes this ring buffer will take.
    pub fn byte_size(&self) -> usize {
        // Header + Availability Array + Ring Buffer
        let size = self.first_buffer_offset()
            + (self.header().num_buffers * self.header().buffer_size) as usize;
        size
    }

    pub fn availability_array_offset(&self) -> usize {
        self.offset + 32
    }

    pub fn first_buffer_offset(&self) -> usize {
        self.offset + 32 + (4 * self.header().num_buffers) as usize
    }

    fn ring_buffer_index(&self, idx: i64) -> usize {
        // TODO - optimise this.
        // We can force power-of-two and use a mask on the integer.
        (idx % self.header().num_buffers) as usize
    }

    /// Checks whether a given ring buffer is avialable to read.
    /// Note: This uses an atomic operation.
    fn is_read_available(&self, idx: i64) -> bool {
        let flag = ((idx as u32) >> self.shift) as i32;
        let ring_index = self.ring_buffer_index(idx);
        self.availability_array()[ring_index].load(Ordering::Acquire) == flag
    }

    /// Marks a buffer as availabel to read.
    #[cfg(test)]
    fn set_read_available(&self, idx: i64) {
        let shift = (self.header().num_buffers as i32).ilog2();
        let ring_index = self.ring_buffer_index(idx);
        let flag = ((idx as u32) >> shift) as i32;
        self.availability_array()[ring_index].store(flag, Ordering::Release);
    }

    /// Returns a ring buffer entry that we can use as a byte slice.
    fn entry<'a>(&'a self, idx: i64) -> RingBufferEntry<'a> {
        let offset_to_ring = self.first_buffer_offset();
        let ring_index = self.ring_buffer_index(idx);
        let start_byte_idx = offset_to_ring + (ring_index * (self.header().buffer_size as usize));
        let end_byte_idx = start_byte_idx + (self.header().buffer_size as usize);
        RingBufferEntry {
            data: &self.data,
            start_offset: start_byte_idx,
            end_offset: end_byte_idx,
        }
    }

    /// Returns a mutable entry for writing.
    #[cfg(test)]
    fn entry_mut<'a>(&'a mut self, idx: i64) -> RingBufferEntryMut<'a> {
        let offset_to_ring = self.first_buffer_offset();
        let ring_index = self.ring_buffer_index(idx);
        let start_byte_idx = offset_to_ring + (ring_index * (self.header().buffer_size as usize));
        let end_byte_idx = start_byte_idx + (self.header().buffer_size as usize);
        RingBufferEntryMut {
            data: &mut self.data,
            start_offset: start_byte_idx,
            end_offset: end_byte_idx,
        }
    }
}

struct RingBufferEntry<'a> {
    data: &'a MmapMut,
    start_offset: usize,
    end_offset: usize,
}
impl<'a> Deref for RingBufferEntry<'a> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.data[self.start_offset..self.end_offset]
    }
}

#[cfg(test)]
struct RingBufferEntryMut<'a> {
    data: &'a mut MmapMut,
    start_offset: usize,
    end_offset: usize,
}

#[cfg(test)]
impl<'a> Deref for RingBufferEntryMut<'a> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.data[self.start_offset..self.end_offset]
    }
}

#[cfg(test)]
impl<'a> DerefMut for RingBufferEntryMut<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data[self.start_offset..self.end_offset]
    }
}

/// This first 32 bytes of any ringbuffer in OTLP-MMAP has this format.
/// We use this struct to "reinterpret_cast" and use memory safe primitives for access.
#[repr(C)]
struct RawRingBufferHeader {
    /// Number of buffers in the ring.
    num_buffers: i64,
    /// Size (in bytes) of each buffer
    buffer_size: i64,
    /// Number of events that have been read.
    reader_index: AtomicI64,
    /// Number of events claimed by writers.
    writer_index: AtomicI64,
}

#[cfg(test)]
mod test {
    use crate::sdk_mmap::data::any_value::Value;
    use crate::sdk_mmap::data::AnyValue;
    use crate::sdk_mmap::{ringbuffer::RawRingBuffer, Error};
    use memmap2::MmapOptions;
    use std::{fs::OpenOptions, sync::Arc};
    use tokio::{sync::RwLock, task::JoinHandle};

    #[tokio::test]
    async fn test_read_and_write() -> Result<(), Error> {
        // TODO - Make sure tempfile works appropriately.
        let path = tempfile::NamedTempFile::new()?;
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path.path())?;
        let desired_len = 512 * 8 + 64 + 128;
        f.set_len(desired_len)?;
        let data = unsafe { MmapOptions::new().map_mut(&f)? };
        let buffer = Arc::new(RwLock::new(RawRingBuffer::new_for_write(data, 0, 512, 8)));
        let read_buffer = buffer.clone();
        let publish: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
            for i in 0..1000 {
                let mut done = false;
                while !done {
                    {
                        let mut ring = buffer.write().await;
                        let value = AnyValue {
                            value: Some(Value::StringValue(format!("{i}"))),
                        };
                        done = ring.try_write(&value)?;
                    }
                    tokio::task::yield_now().await;
                }
            }
            Ok(())
        });
        let consume: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
            for i in 0..1000 {
                let mut done = false;
                while !done {
                    {
                        let ring = read_buffer.read().await;
                        if let Some(value) = ring.try_read::<AnyValue>()? {
                            if let AnyValue {
                                value: Some(Value::StringValue(sv)),
                            } = value
                            {
                                assert_eq!(sv, format!("{i}"));
                            } else {
                                panic!("Expected string value, found: {value:?}")
                            }
                            done = true
                        }
                    }
                    tokio::task::yield_now().await;
                }
            }
            Ok(())
        });
        // Propogate errors and wait for complete.
        let (r1, r2) = tokio::try_join!(publish, consume)?;
        r1?;
        r2?;
        Ok(())
    }
}
