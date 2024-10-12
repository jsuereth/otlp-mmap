use std::ops::Deref;
use std::thread;
use std::{fs::OpenOptions, path::Path, sync::atomic::AtomicI64};
use memmap::MmapOptions;
use std::sync::atomic::Ordering;

/// An input channel that leverages mmap'd files to communicate events via ringbuffer.
pub struct RingbufferInputChannel {
    // We own file to keep its lifetime.
    #[allow(dead_code)]
    f: std::fs::File,
    data: memmap::MmapMut,
}

impl RingbufferInputChannel {
    /// Construct a new Ringbuffer file input using the given path.
    pub fn new(path: &Path) -> RingbufferInputChannel {
        let f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(false)
        .open(path)
        .expect("Unable to open file");
        let data = unsafe {
            MmapOptions::new()
                .map_mut(&f)
                .expect("Could not access data from memory mapped file")
        };
        RingbufferInputChannel { f, data }
    }
    /// Read the next event in the ringbuffer.
    /// Note: this will block!
    /// 
    /// Additionally, you cannot read the next chunk until this is "dropped".
    pub fn next<'a>(&'a mut self) -> RingbufferChunk<'a> {
        // TODO - Check sanity of the stream before continuing.
        // TODO - make sure previous chunk was returned before continuing...
        // TODO - exponential backoff loop.
        while !self.state().has_messages() {
            thread::yield_now();
        }
        let read_idx = self.read_position();
        RingbufferChunk {
            data: &self.data,
            header: unsafe {
                &mut *(self.data.as_ref().as_ptr() as *mut RingBufferHeader)
            },
            read_idx,
        }
    }
    fn read_position(&self) -> i64 {
        self.state().read_position.load(Ordering::Relaxed)
    }
    /// Grants readable access to the ring buffer header.
    fn state(&self) -> &RingBufferHeader {
        unsafe { &*(self.data.as_ref().as_ptr() as *const RingBufferHeader)}
    }
    /// Returns the header of this file.
    pub fn version(&self) -> i64 {
        self.state().version
    }
    // TODO - helper to move to next buf and read it...
}

/// Grants access to memory chunk in a ringbuffer.
pub struct RingbufferChunk<'a> {
    data: &'a memmap::MmapMut,
    header: &'a mut RingBufferHeader,
    read_idx: i64,
}
impl <'a> Drop for RingbufferChunk<'a> {
    fn drop(&mut self) {
        self.header.move_next_chunk(self.read_idx);
    }
}
impl <'a> Deref for RingbufferChunk<'a> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        let start_byte_idx = 64 + (self.read_idx*self.header.chunk_size) as usize;
        let end_byte_idx = 64 + ((self.read_idx+1)*self.header.chunk_size) as usize;
        &self.data[start_byte_idx..end_byte_idx]
    }
}
impl <'a> prost::bytes::Buf for RingbufferChunk<'a> {
    fn remaining(&self) -> usize {
        todo!()
    }

    fn chunk(&self) -> &[u8] {
        todo!()
    }

    fn advance(&mut self, cnt: usize) {
        todo!()
    }
}



/// This first 64 bytes of any ringbuffer in OTLP-MMAP has this format.
/// We use this struct to "reinterpret_cast" and use memory safe primitives for access.
#[repr(C)]
pub(crate) struct RingBufferHeader {
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

impl RingBufferHeader {
    fn move_next_chunk(&mut self, expected: i64) {
        let next = (expected+1) % self.num_chunks;
        match self.read_position.compare_exchange(expected, next, Ordering::SeqCst, Ordering::Relaxed) {
            Ok(_) => (), // ignore
            Err(_) => {}, // TODO - mark this input stream as no longer sane (multiple readers) and refresh.
        }
    }

    fn has_messages(&self) -> bool {
        let start = self.read_position.load(Ordering::SeqCst);
        let end = self.write_position.load(Ordering::SeqCst);
        start != end
    }
}