mod oltp_mmap;

use std::path::Path;
use oltp_mmap::OtlpInputCommon;


fn main() {
    let path = Path::new("..").join("export");
    println!("Reading {path:?}");
    let mut otlp = OtlpInputCommon::new(&path);

    // TOOD - actually read the data.
    let mut idx = 0;
    loop {
        println!("Reading message #: {idx}");
        let _ = otlp.next_span();
        // sleep(time::Duration::from_secs(1));
        idx += 1;
    }
}
