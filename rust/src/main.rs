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
        let next = (start+1) % (self.num_chunks+1);
        next != end && self.read_position.compare_exchange(start, next, Ordering::SeqCst, Ordering::Relaxed).is_ok()
    }
    fn move_next_chunk(&mut self) {
        // TOOD - Exponential backoff and error handling.
        while !self.try_move_next_chunk() {
            yield_now();
        }
    }
}

fn main() {
    let path = Path::new("..\\export.meta");
    println!("Reading {path:?}");
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
    let state_ref = unsafe { &mut *(data.as_ref().as_ptr() as *mut Meta) };
    println!("Read\nVersion: {}\nChunk Size: {}\nNum Chunks: {}", state_ref.version, state_ref.chunk_size, state_ref.num_chunks);
    println!("Reader index: {}", state_ref.read_position.load(Ordering::Relaxed));
    println!("Writer index: {}", state_ref.write_position.load(Ordering::Relaxed));

    // TOOD - actually read the data.
    let mut idx = 1;
    loop {
        state_ref.move_next_chunk();
        let read_idx = state_ref.read_position.load(Ordering::Relaxed);
        println!("Reading message #: {idx} @ {read_idx}");
        let start_byte_idx = (read_idx*state_ref.chunk_size) as usize;
        let stop_byte_idx = ((read_idx+1)*state_ref.chunk_size) as usize;

        let chunk = &data[start_byte_idx..stop_byte_idx];
        if let Ok(msg) = str::from_utf8(chunk) {
            println!(" - Read [{msg}]");
        } else {
            println!(" - Failed to read msg!");
        }
        // sleep(time::Duration::from_secs(1));
        idx += 1;
    }
}
