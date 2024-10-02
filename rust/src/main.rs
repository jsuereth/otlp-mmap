use core::str;
use core::time;
use std::cell::Ref;
use std::ops::Deref;
use std::thread;
use std::{fs::OpenOptions, path::Path, sync::atomic::AtomicI64};
use memmap::MmapOptions;
use std::sync::atomic::Ordering;
use std::thread::yield_now;
use std::thread::sleep;


#[repr(C)]
pub(crate) struct ChannelHeader {
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

impl ChannelHeader {
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

pub struct InputChannel {
    // We own file to keep its lifetime.
    f: std::fs::File,
    data: memmap::MmapMut,
}

impl InputChannel {
    pub fn new(path: &Path) -> InputChannel {
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
        InputChannel { f, data }
    }
    pub fn next<'a>(&'a mut self) -> NextBuf<'a> {
        // TODO - Check sanity of the stream before continuing.
        // TODO - exponential backoff loop.
        while !self.state().has_messages() {
            thread::yield_now();
        }
        let read_idx = self.read_position();
        NextBuf {
            data: &self.data,
            header: unsafe {
                &mut *(self.data.as_ref().as_ptr() as *mut ChannelHeader)
            },
            read_idx,
        }
    }
    fn read_position(&self) -> i64 {
        self.state().read_position.load(Ordering::Relaxed)
    }
    fn write_position(&self) -> i64 {
        self.state().write_position.load(Ordering::Relaxed)
    }
    fn state(&self) -> &ChannelHeader {
        unsafe { &*(self.data.as_ref().as_ptr() as *const ChannelHeader)}
    }
    // TODO - helper to move to next buf and read it...
}

pub struct NextBuf<'a> {
    data: &'a memmap::MmapMut,
    header: &'a mut ChannelHeader,
    read_idx: i64,
}
impl <'a> Drop for NextBuf<'a> {
    fn drop(&mut self) {
        self.header.move_next_chunk(self.read_idx);
    }
}
impl <'a> Deref for NextBuf<'a> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        let start_byte_idx = 64 + (self.read_idx*self.header.chunk_size) as usize;
        let end_byte_idx = 64 + ((self.read_idx+1)*self.header.chunk_size) as usize;
        &self.data[start_byte_idx..end_byte_idx]
    }
}

fn main() {
    let path = Path::new("..\\export.meta");
    println!("Reading {path:?}");
    let mut channel = InputChannel::new(path);

    println!("Read\nVersion: {}\nChunk Size: {}\nNum Chunks: {}", channel.state().version, channel.state().chunk_size, channel.state().num_chunks);
    println!("Reader index: {}", channel.read_position());
    println!("Writer index: {}", channel.write_position());

    // TOOD - actually read the data.
    let mut idx = 0;
    loop {
        //println!("Reading message #: {idx}");
        if let Ok(msg) = str::from_utf8(&channel.next()) {
            println!(" - Read idx[{idx}] w/ [{msg}]");
            ()
        } else {
            println!(" - Failed to read msg {idx}!");
        }
        // sleep(time::Duration::from_secs(1));
        idx += 1;
    }
}
