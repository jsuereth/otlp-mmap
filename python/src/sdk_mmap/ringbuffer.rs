//! Multi-Producer, single-consumer ring buffer.

use std::{
    marker::PhantomData,
    ops::Deref,
    sync::atomic::{AtomicI32, AtomicI64, Ordering},
    sync::Mutex,
};

use memmap2::MmapMut;
use super::Error;

/// Synchronous access to RingBuffer inputs.
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

    /// Reads the next input if available.
    pub fn try_read_next(&self) -> Result<Option<T>, Error> {
        self.input.lock().unwrap().try_read::<T>()
    }
}

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

    /// Checks to see if we can read the next available buffer.
    fn try_obtain_read_idx(&self) -> Option<i64> {
        let next = self.header().reader_index.load(Ordering::Acquire) + 1;
        if self.is_read_available(next) {
            Some(next)
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

    pub fn availability_array_offset(&self) -> usize {
        self.offset + 32
    }

    pub fn first_buffer_offset(&self) -> usize {
        self.offset + 32 + (4 * self.header().num_buffers) as usize
    }

    fn ring_buffer_index(&self, idx: i64) -> usize {
        // TODO - optimise this.
        (idx % self.header().num_buffers) as usize
    }

    /// Checks whether a given ring buffer is avialable to read.
    fn is_read_available(&self, idx: i64) -> bool {
        let flag = ((idx as u32) >> self.shift) as i32;
        let ring_index = self.ring_buffer_index(idx);
        self.availability_array()[ring_index].load(Ordering::Acquire) == flag
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

/// This first 32 bytes of any ringbuffer in OTLP-MMAP has this format.
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
