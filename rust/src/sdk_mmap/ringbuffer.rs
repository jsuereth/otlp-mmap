//! Multi-Producer, single-consumer ring buffer
//
// A RingBuffer is structured as follows:
// | Header | Availability Array | Buffer1 | ... | BufferN |

use std::{
    marker::PhantomData,
    ops::Deref,
    sync::atomic::{AtomicI32, AtomicI64, Ordering},
    time::Duration,
};

use memmap::MmapMut;
use tokio::sync::Mutex;

use crate::oltp_mmap::Error;

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
                return Ok(result);
            } else {
                tokio::task::yield_now().await;
            }
        }
        // Sleep spin, exponentially slower.
        let mut d = Duration::from_millis(1);
        loop {
            if let Some(result) = input.try_read::<T>()? {
                return Ok(result);
            } else {
                println!("Waiting {d:?} for input...");
                tokio::time::sleep(d).await;
            }
            // TODO - Cap max wait time configuration.
            if d.as_secs() < 1 {
                d = d * 2;
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

    /// The ring buffer header (with atomic access).
    fn header(&self) -> &RawRingBufferHeader {
        unsafe { &*(self.data.as_ref().as_ptr().add(self.offset) as *const RawRingBufferHeader) }
    }
    /// The availability array for ring buffer entries.
    fn availability_array(&self) -> &[AtomicI32] {
        unsafe {
            let start_ptr = self.data.as_ref().as_ptr().add(self.offset + 32).cast::<AtomicI32>();
            std::slice::from_raw_parts(start_ptr, self.header().num_buffers as usize)
        }
    }
    /// The number of bytes this ring buffer will take.
    pub fn byte_size(&self) -> usize {
        // Header + Availability Array + Ring Buffer
        let size = 32
            + (4 * self.header().num_buffers)
            + (self.header().num_buffers * self.header().buffer_size);
        size as usize
    }

    fn ring_buffer_index(&self, idx: i64) -> usize {
        // TODO - optimise this.
        // We can force power-of-two and use a mask on the integer.
        (idx % self.header().num_buffers) as usize
    }

    /// Checks whether a given ring buffer is avialable to read.
    /// Note: This uses an atomic operation.
    fn is_read_available(&self, idx: i64) -> bool {
        println!("Checking if we can read: {idx}");
        let flag = ((idx as u32) >> self.shift) as i32;
        let ring_index = self.ring_buffer_index(idx);
        self.availability_array()[ring_index].load(Ordering::Acquire) == flag
    }

    /// Marks a buffer as availabel to read.
    fn set_read_available(&self, idx: i64) {
        let shift = (self.header().num_buffers as i32).ilog2();
        let ring_index = self.ring_buffer_index(idx);
        let flag = ((idx as u32) >> shift) as i32;
        self.availability_array()[ring_index].store(flag, Ordering::Release);
    }

    /// Returns a ring buffer entry that we can use as a byte slice.
    fn entry<'a>(&'a self, idx: i64) -> RingBufferEntry<'a> {
        let ring_index = self.ring_buffer_index(idx);
        println!("Reading: {idx} - real idx {ring_index}");
        let start_byte_idx = 64 + ring_index * (self.header().buffer_size as usize);
        let end_byte_idx = 64 + ((ring_index + 1) * (self.header().buffer_size as usize));
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
