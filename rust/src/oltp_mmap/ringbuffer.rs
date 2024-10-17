use super::Error;
use memmap::MmapOptions;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::{fs::OpenOptions, path::Path, sync::atomic::AtomicI64};
use tokio::sync::Mutex;

/// Async access to RingBuffer input files.
pub struct RingBufferReader<T> {
    input: Mutex<RawRingbufferReader>,
    phantom: PhantomData<T>,
}

impl<T> RingBufferReader<T>
where
    T: prost::Message + std::default::Default + 'static,
{
    /// Construct a new Ringbuffer file input using the given path.
    pub fn new(path: &Path) -> Result<RingBufferReader<T>, Error> {
        Ok(RingBufferReader {
            input: Mutex::new(RawRingbufferReader::new(path)?),
            phantom: PhantomData,
        })
    }

    /// Reads the next input on this ringbuffer.
    /// Note: This will lock the ringbuffer from access for the duration.
    pub async fn next(&self) -> Result<T, Error> {
        // We want to ensure "chunk" falls out of scope (is Drop-ed) before this
        // lock is dropped.
        let mut input = self.input.lock().await;
        // Exponential Backoff attempt to read next span.
        // TODO -  FastSpin ~ 10 times?
        // Yield-Spin ~ 10 times
        for _ in 0..10 {
            if let Some(buf) = input.try_next() {
                return Ok(T::decode_length_delimited(buf.deref())?);
            } else {
                tokio::task::yield_now().await;
            }
        }
        // Sleep spin, exponentially slower.
        let mut d = Duration::from_millis(1);
        loop {
            if let Some(buf) = input.try_next() {
                return Ok(T::decode_length_delimited(buf.deref())?);
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

    /// Returns the timestamp of this ringbuffer.
    pub async fn version(&self) -> i64 {
        self.input.lock().await.version()
    }
}

/// An input channel that leverages mmap'd files to communicate events via ringbuffer.
///
/// All interactions are synchronous.
pub struct RawRingbufferReader {
    // We own file to keep its lifetime.
    #[allow(dead_code)]
    f: std::fs::File,
    data: memmap::MmapMut,
}

impl RawRingbufferReader {
    /// Construct a new Ringbuffer file input using the given path.
    fn new(path: &Path) -> Result<RawRingbufferReader, Error> {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(path)?;
        let data = unsafe {
            MmapOptions::new()
                .map_mut(&f)
                .expect("Could not access data from memory mapped file")
        };
        Ok(RawRingbufferReader { f, data })
    }

    /// Read the next event in the ringbuffer.
    /// Returns None if no messages are yet available.
    fn try_next<'a>(&'a mut self) -> Option<RawRingbufferEntry<'a>> {
        // TODO - Check sanity of the stream before continuing.
        // TODO - make sure previous chunk was returned before continuing...
        if !self.state().has_messages() {
            None
        } else {
            let read_idx = self.read_position();
            Some(RawRingbufferEntry {
                data: &self.data,
                header: unsafe { &mut *(self.data.as_ref().as_ptr() as *mut RawRingBufferHeader) },
                read_idx,
            })
        }
    }
    fn read_position(&self) -> i64 {
        self.state().read_position.load(Ordering::Relaxed)
    }
    /// Grants readable access to the ring buffer header.
    fn state(&self) -> &RawRingBufferHeader {
        unsafe { &*(self.data.as_ref().as_ptr() as *const RawRingBufferHeader) }
    }
    /// Returns the header of this file.
    pub fn version(&self) -> i64 {
        self.state().version
    }
    // TODO - helper to move to next buf and read it...
}

/// Grants access to memory chunk in a ringbuffer.
struct RawRingbufferEntry<'a> {
    data: &'a memmap::MmapMut,
    header: &'a mut RawRingBufferHeader,
    read_idx: i64,
}
impl<'a> Drop for RawRingbufferEntry<'a> {
    fn drop(&mut self) {
        self.header.move_next_chunk(self.read_idx);
    }
}
impl<'a> Deref for RawRingbufferEntry<'a> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        let start_byte_idx = 64 + (self.read_idx * self.header.chunk_size) as usize;
        let end_byte_idx = 64 + ((self.read_idx + 1) * self.header.chunk_size) as usize;
        &self.data[start_byte_idx..end_byte_idx]
    }
}

/// This first 64 bytes of any ringbuffer in OTLP-MMAP has this format.
/// We use this struct to "reinterpret_cast" and use memory safe primitives for access.
#[repr(C)]
pub(crate) struct RawRingBufferHeader {
    /// Current data instance version
    version: i64,
    num_chunks: i64,
    chunk_size: i64,
    ignore: i64,
    ignore_2: i64,
    checksum: i64,
    read_position: AtomicI64,
    write_position: AtomicI64,
}

impl RawRingBufferHeader {
    fn move_next_chunk(&mut self, expected: i64) {
        let next = (expected + 1) % self.num_chunks;
        match self.read_position.compare_exchange(
            expected,
            next,
            Ordering::SeqCst,
            Ordering::Relaxed,
        ) {
            Ok(_) => (), // ignore
            Err(_) => {} // TODO - mark this input stream as no longer sane (multiple readers) and refresh.
        }
    }

    fn has_messages(&self) -> bool {
        let start = self.read_position.load(Ordering::SeqCst);
        let end = self.write_position.load(Ordering::SeqCst);
        start != end
    }
}
