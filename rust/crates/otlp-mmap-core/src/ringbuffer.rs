//! Ringbuffers in MMAP file protocol.

use crate::Error;
use memmap2::MmapMut;
use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicI32, AtomicI64, Ordering},
};

// TODO - make this typed?

/// Reads typed messages from a ring buffer.
pub trait RingBufferReader<T> {
    /// Attempts to read a message from a ringbuffer.
    ///
    /// Returns None if the ringbuffer is empty or otherwise unavailable.
    fn try_read(&self) -> Result<Option<T>, Error>;
}

/// Writes types messages to a ring buffer.
pub trait RingBufferWriter<T> {
    /// Attempts to write a message to a ringbuffer.
    ///
    /// Returns false if the ringbuffer is full or otherwise unavailable.
    fn try_write(&mut self, msg: &T) -> Result<bool, Error>;
}

/// A wrapper around the underlying Ringbuffer to safely expose read/write methods.
struct RingBufferWraper<T> {
    ring: RingBuffer,
    _phantom: PhantomData<T>,
}

impl<T: prost::Message + std::fmt::Debug> RingBufferWriter<T> for RingBufferWraper<T> {
    fn try_write(&mut self, msg: &T) -> Result<bool, Error> {
        self.ring.try_write(msg)
    }
}

impl<T: prost::Message + Default> RingBufferReader<T> for RingBufferWraper<T> {
    fn try_read(&self) -> Result<Option<T>, Error> {
        self.ring.try_read()
    }
}

/// A mmap ringbuffer implementation.
///
/// Note: This is currently designed to only allow ONE consumer
///       but multiple prodcuers.
pub struct RingBuffer {
    /// The mmap data
    data: MmapMut,
    /// The offset into the mmap data where the ringbuffer starts.
    offset: usize,
    /// Efficient mechanism to convert a message index into
    /// an availability flag.  Effectively - size.ilog2()
    shift: u32,
}

impl RingBuffer {
    /// Constructs a new reader of ring buffers.
    pub fn reader<T: prost::Message + Default>(
        data: MmapMut,
        offset: usize,
    ) -> impl RingBufferReader<T> {
        RingBufferWraper {
            ring: Self::new(data, offset),
            _phantom: PhantomData,
        }
    }
    /// Constructs a new writer of ring buffers.
    pub fn writer<T: prost::Message + std::fmt::Debug>(
        data: MmapMut,
        offset: usize,
        buffer_size: usize,
        num_buffers: usize,
    ) -> impl RingBufferWriter<T> {
        RingBufferWraper {
            ring: Self::new_for_write(data, offset, buffer_size, num_buffers),
            _phantom: PhantomData,
        }
    }

    /// Constructs a new ring buffer on an mmap at the offset.
    fn new(data: MmapMut, offset: usize) -> RingBuffer {
        let hdr = unsafe { &*(data.as_ref().as_ptr().add(offset) as *const RingBufferHeader) };
        RingBuffer {
            data,
            offset,
            shift: (hdr.num_buffers as u32).ilog2(),
        }
    }

    /// Constructs a new ring buffer, but sets the header values for it as well.
    fn new_for_write(
        mut data: MmapMut,
        offset: usize,
        buffer_size: usize,
        num_buffers: usize,
    ) -> RingBuffer {
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

    /// Attempts to read a protobuf meesage from the ringbuffer.
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

    /// Attempst to write a protobuf message to the ringbuffer.
    fn try_write<T: prost::Message + std::fmt::Debug>(&mut self, msg: &T) -> Result<bool, Error> {
        if let Some(idx) = self.try_obtain_write_idx() {
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
    fn header(&self) -> &RingBufferHeader {
        unsafe { &*(self.data.as_ref().as_ptr().add(self.offset) as *const RingBufferHeader) }
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
    fn byte_size(&self) -> usize {
        // Header + Availability Array + Ring Buffer
        let size = self.first_buffer_offset()
            + (self.header().num_buffers * self.header().buffer_size) as usize;
        size
    }

    fn availability_array_offset(&self) -> usize {
        self.offset + 32
    }

    fn first_buffer_offset(&self) -> usize {
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

    /// Marks a buffer as available to read.
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

struct RingBufferEntryMut<'a> {
    data: &'a mut MmapMut,
    start_offset: usize,
    end_offset: usize,
}

impl<'a> Deref for RingBufferEntryMut<'a> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.data[self.start_offset..self.end_offset]
    }
}

impl<'a> DerefMut for RingBufferEntryMut<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data[self.start_offset..self.end_offset]
    }
}

/// This first 32 bytes of any ringbuffer in OTLP-MMAP has this format.
/// We use this struct to "reinterpret_cast" and use memory safe primitives for access.
#[repr(C)]
struct RingBufferHeader {
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
    use crate::{ringbuffer::RingBuffer, Error};
    use memmap2::MmapOptions;
    use otlp_mmap_protocol::any_value::Value;
    use otlp_mmap_protocol::AnyValue;
    use std::sync::atomic::Ordering;
    use std::{fs::OpenOptions, sync::Arc};

    /// A helper to create a RingBuffer for testing with a specific state.
    struct TestRingBuffer {
        // Keep file alive for the duration of the test
        _file: tempfile::NamedTempFile,
        pub buffer: RingBuffer,
    }

    #[derive(Clone, Copy)]
    struct TestRingBufferOptions {
        num_buffers: usize,
        buffer_size: usize,
        reader_idx: i64,
        writer_idx: i64,
    }
    impl Default for TestRingBufferOptions {
        fn default() -> Self {
            Self {
                num_buffers: 8,
                buffer_size: 64,
                reader_idx: -1,
                writer_idx: -1,
            }
        }
    }

    impl TestRingBuffer {
        fn new(opts: TestRingBufferOptions) -> TestRingBuffer {
            let file = tempfile::NamedTempFile::new().unwrap();
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(file.path())
                .unwrap();
            // Calculate required size. Header + availability array + buffers
            let header_size = 32;
            let availability_array_size = opts.num_buffers * 4;
            let buffers_size = opts.num_buffers * opts.buffer_size;
            let total_size = header_size + availability_array_size + buffers_size;
            f.set_len(total_size as u64).unwrap();
            let data = unsafe { MmapOptions::new().map_mut(&f).unwrap() };

            let buffer = RingBuffer::new_for_write(data, 0, opts.buffer_size, opts.num_buffers);
            buffer
                .header()
                .reader_index
                .store(opts.reader_idx, Ordering::SeqCst);
            buffer
                .header()
                .writer_index
                .store(opts.writer_idx, Ordering::SeqCst);

            TestRingBuffer {
                _file: file,
                buffer,
            }
        }
    }

    #[test]
    fn test_ring_buffer_index() {
        let test_buffer = TestRingBuffer::new(TestRingBufferOptions::default());
        let ring = &test_buffer.buffer;

        assert_eq!(ring.ring_buffer_index(0), 0);
        assert_eq!(ring.ring_buffer_index(7), 7);
        assert_eq!(ring.ring_buffer_index(8), 0, "should wrap around");
        assert_eq!(ring.ring_buffer_index(15), 7, "should wrap around");
    }

    #[test]
    fn test_is_and_set_read_available() {
        // The number of buffers must be a power of two for the shift logic to be correct.
        let test_buffer = TestRingBuffer::new(TestRingBufferOptions::default());
        let ring = &test_buffer.buffer;
        assert_eq!(ring.shift, 3);

        let idx = 10; // ring index 2
        let flag = (idx as u32 >> ring.shift) as i32; // 10 >> 3 = 1
        assert_eq!(flag, 1);

        assert!(
            !ring.is_read_available(idx),
            "should not be available initially"
        );
        ring.set_read_available(idx);
        assert!(ring.is_read_available(idx), "should be available after set");

        // Check another index that maps to the same ring slot but needs a different flag
        let idx2 = idx + (1 << ring.shift); // 10 + 8 = 18, ring index 2
        let flag2 = (idx2 as u32 >> ring.shift) as i32; // 18 >> 3 = 2
        assert_eq!(flag2, 2);
        assert!(
            !ring.is_read_available(idx2),
            "should not be available, as flag is different"
        );
    }

    #[test]
    fn test_try_obtain_write_idx() {
        // Buffer with space
        let test_buffer_space = TestRingBuffer::new(TestRingBufferOptions {
            writer_idx: 4,
            ..Default::default()
        });
        let ring_space = &test_buffer_space.buffer;
        assert_eq!(ring_space.try_obtain_write_idx(), Some(5));
        assert_eq!(ring_space.header().writer_index.load(Ordering::SeqCst), 5);

        // Buffer full condition
        // Writer is at 7, reader is at 0. `has_capacity` is `(7 + 1 - 8) < 0` which is `0 < 0` -> false.
        // So the buffer is considered full when writer_index == reader_index + num_buffers - 1
        let test_buffer_full = TestRingBuffer::new(TestRingBufferOptions {
            writer_idx: 7,
            reader_idx: 0,
            ..Default::default()
        });
        let ring_full = &test_buffer_full.buffer;
        assert_eq!(
            ring_full.try_obtain_write_idx(),
            None,
            "buffer should be full"
        );
    }

    #[test]
    fn test_try_obtain_read_idx() {
        // Happy path
        let test_buffer_happy = TestRingBuffer::new(TestRingBufferOptions {
            reader_idx: 4,
            writer_idx: 5,
            ..Default::default()
        });
        let ring_happy = &test_buffer_happy.buffer;
        ring_happy.set_read_available(5);
        assert_eq!(ring_happy.try_obtain_read_idx(), Some(5));

        // Empty (reader has caught up to writer)
        let test_buffer_empty = TestRingBuffer::new(TestRingBufferOptions {
            reader_idx: 4,
            writer_idx: 4,
            ..Default::default()
        });
        let ring_empty = &test_buffer_empty.buffer;
        assert_eq!(ring_empty.try_obtain_read_idx(), None);

        // Not yet available
        let test_buffer_not_ready = TestRingBuffer::new(TestRingBufferOptions {
            reader_idx: 4,
            writer_idx: 5,
            ..Default::default()
        });
        let ring_not_ready = &test_buffer_not_ready.buffer;
        // writer is at 5, but `set_read_available(5)` has not been called
        assert!(!ring_not_ready.is_read_available(5));
        assert_eq!(ring_not_ready.try_obtain_read_idx(), None);
    }

    // TODO - test read then write
    //  #[tokio::test]
    // async fn test_read_and_write() -> Result<(), Error> {
    //     let options = TestRingBufferOptions {
    //         num_buffers: 8,
    //         buffer_size: 512,
    //         ..Default::default()
    //     };
    //     let buffer = Arc::new(RwLock::new(TestRingBuffer::new(options.clone())));
    //     let read_buffer = buffer.clone();
    //     let publish: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
    //         for i in 0..1000 {
    //             let mut done = false;
    //             while !done {
    //                 {
    //                     let mut ring = buffer.write().await;
    //                     let value = AnyValue {
    //                         value: Some(Value::StringValue(format!("{i}"))),
    //                     };
    //                     done = ring.buffer.try_write(&value)?;
    //                 }
    //                 tokio::task::yield_now().await;
    //             }
    //         }
    //         Ok(())
    //     });
    //     let consume: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
    //         for i in 0..1000 {
    //             let mut done = false;
    //             while !done {
    //                 {
    //                     let ring = read_buffer.read().await;
    //                     if let Some(value) = ring.buffer.try_read::<AnyValue>()? {
    //                         if let AnyValue {
    //                             value: Some(Value::StringValue(sv)),
    //                         } = value
    //                         {
    //                             assert_eq!(sv, format!("{i}"));
    //                         } else {
    //                             panic!("Expected string value, found: {value:?}")
    //                         }
    //                         done = true
    //                     }
    //                 }
    //                 tokio::task::yield_now().await;
    //             }
    //         }
    //         Ok(())
    //     });
    //     // Propogate errors and wait for complete.
    //     let (r1, r2) = tokio::try_join!(publish, consume)?;
    //     r1?;
    //     r2?;
    //     Ok(())
    // }
}
