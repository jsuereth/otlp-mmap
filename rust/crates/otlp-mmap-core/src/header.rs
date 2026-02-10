//! OTLP-MMAP Core - Header processing

use crate::Error;
use memmap2::{MmapMut, MmapOptions};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

/// Header of the MMap File.  We use this to check sanity / change of the overall file.
pub(crate) struct MmapHeader {
    data: MmapMut,
}

impl MmapHeader {
    pub(crate) fn new<F>(file: F) -> Result<MmapHeader, Error>
    where
        F: memmap2::MmapAsRawDesc,
    {
        Ok(MmapHeader {
            data: unsafe { MmapOptions::new().offset(0).len(64).map_mut(file)? },
        })
    }

    fn raw(&self) -> &RawMmapHeader {
        unsafe { &*(self.data.as_ref().as_ptr() as *const RawMmapHeader) }
    }

    /// Version of the MMAP file.
    pub fn version(&self) -> i64 {
        self.raw().version
    }
    /// The start time of the MMAP file in nanoseconds since epoch.
    /// Note: This uses atomic Ordering::Acquire.
    pub fn start_time(&self) -> u64 {
        self.raw().start_time_unix_nano.load(Ordering::Acquire)
    }
    /// Offset in MMAP file where event ringbuffer starts.
    pub fn events_offset(&self) -> i64 {
        self.raw().events.load(Ordering::Relaxed)
    }
    /// Offset in MMAP file where span ringbuffer starts.
    pub fn spans_offset(&self) -> i64 {
        self.raw().spans.load(Ordering::Relaxed)
    }
    /// Offset in MMAP file where measurement ringbuffer starts.
    pub fn measurements_offset(&self) -> i64 {
        self.raw().measurements.load(Ordering::Relaxed)
    }
    /// Offset in MMAP file where dictionary starts.
    pub fn dictionary_offset(&self) -> i64 {
        self.raw().dictionary.load(Ordering::Relaxed)
    }
}

#[repr(C)]
struct RawMmapHeader {
    /// Version of the file.
    version: i64,
    /// Location of logs event buffer.
    events: AtomicI64,
    /// Location of spans event buffer.
    spans: AtomicI64,
    /// Location of measurements event buffer.
    measurements: AtomicI64,
    /// Location of dictionary.
    dictionary: AtomicI64,
    /// Start timestamp.
    start_time_unix_nano: AtomicU64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{File, OpenOptions};
    use std::io::{Seek, Write};
    use tempfile::NamedTempFile;

    // The header is 64 bytes, but only 40 bytes are used today.
    const HEADER_SIZE: u64 = 64;

    /// Helper to write the main MMAP header.
    fn write_main_header(
        file: &mut File,
        version: i64,
        events: i64,
        spans: i64,
        measurements: i64,
        dictionary: i64,
        start_time: u64,
    ) -> std::io::Result<()> {
        file.seek(std::io::SeekFrom::Start(0))?;
        file.write_all(&version.to_ne_bytes())?;
        file.write_all(&events.to_ne_bytes())?;
        file.write_all(&spans.to_ne_bytes())?;
        file.write_all(&measurements.to_ne_bytes())?;
        file.write_all(&dictionary.to_ne_bytes())?;
        file.write_all(&start_time.to_ne_bytes())?;
        file.flush()
    }

    #[test]
    fn test_mmap_header_accessors() -> Result<(), Error> {
        let file = NamedTempFile::new()?;
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())?;
        f.set_len(1024)?;

        write_main_header(&mut f, 1, 100, 200, 300, 400, 12345)?;

        let header = MmapHeader::new(&f)?;
        assert_eq!(header.version(), 1);
        assert_eq!(header.events_offset(), 100);
        assert_eq!(header.spans_offset(), 200);
        assert_eq!(header.measurements_offset(), 300);
        assert_eq!(header.dictionary_offset(), 400);
        assert_eq!(header.start_time(), 12345);

        Ok(())
    }
}
