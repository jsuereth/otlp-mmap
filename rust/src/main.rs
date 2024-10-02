mod oltp_mmap;

use core::str;
use std::path::Path;
use oltp_mmap::ringbuffer::RingbufferInputChannel;


fn main() {
    let path = Path::new("..").join("export.meta");
    println!("Reading {path:?}");
    let mut channel = RingbufferInputChannel::new(&path);

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
