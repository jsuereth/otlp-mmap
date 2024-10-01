use core::str;
use core::time;
use std::{fs::OpenOptions, path::Path, sync::atomic::AtomicI64};
use memmap::MmapOptions;
use std::sync::atomic::Ordering;
use std::thread::yield_now;
use std::thread::sleep;


#[repr(C)]
pub(crate) struct Meta {
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

impl Meta {

    fn try_move_next_chunk(&mut self) -> bool {
        let start = self.read_position.load(Ordering::SeqCst);
        let end = self.write_position.load(Ordering::SeqCst);
        let next = (start+1) % self.num_chunks;
        // TODO - we always loose one message if we don't do this, figure out a better algorithm, as this leads to rejected/corrupted data.
        let too_far = (end+1) % self.num_chunks;
        // TODO - we should not read index 0?
        next != too_far && self.read_position.compare_exchange(start, next, Ordering::SeqCst, Ordering::Relaxed).is_ok()
    }
    fn move_next_chunk(&mut self) {
        // TOOD - Exponential backoff and error handling.
        while !self.try_move_next_chunk() {
            yield_now();
        }
    }
}

struct InputChannel {
    // We own file to keep its lifetime.
    f: std::fs::File,
    data: memmap::MmapMut,
}
impl InputChannel {
    fn new(path: &Path) -> InputChannel {
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
    fn state_mut(&mut self) -> &mut Meta {
        unsafe { &mut *(self.data.as_ref().as_ptr() as *mut Meta)}
    }
    fn state(&self) -> &Meta {
        unsafe { &*(self.data.as_ref().as_ptr() as *const Meta)}
    }
    fn read_idx(&self) -> i64 {
        self.state().read_position.load(Ordering::Relaxed) 
    }

    fn current_buf(&self) -> &[u8] {
        let read_idx = self.read_idx();
        let start_byte_idx = 64 + (read_idx*self.state().chunk_size) as usize;
        let stop_byte_idx = 64 + ((read_idx+1)*self.state().chunk_size) as usize;
        &self.data[start_byte_idx..stop_byte_idx]
    }

    // TODO - helper to move to next buf and read it...


}

fn main() {
    let path = Path::new("..\\export.meta");
    println!("Reading {path:?}");
    let mut channel = InputChannel::new(path);

    println!("Read\nVersion: {}\nChunk Size: {}\nNum Chunks: {}", channel.state().version, channel.state().chunk_size, channel.state().num_chunks);
    println!("Reader index: {}", channel.state().read_position.load(Ordering::Relaxed));
    println!("Writer index: {}", channel.state().write_position.load(Ordering::Relaxed));

    // TOOD - actually read the data.
    let mut idx = 1;
    loop {
        //println!("Reading message #: {idx}");
        if let Ok(msg) = str::from_utf8(channel.current_buf()) {
            println!(" - Read idx[{idx}] w/ [{msg}]");
            println!("Reader index: {}", channel.state().read_position.load(Ordering::Relaxed));
            println!("Writer index: {}", channel.state().write_position.load(Ordering::Relaxed));
            ()
        } else {
            println!(" - Failed to read msg {idx}!");
        }
        // sleep(time::Duration::from_secs(1));
        channel.state_mut().move_next_chunk();
        idx += 1;
    }
}
