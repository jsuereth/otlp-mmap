//! OTLP-MMAP Core - Header processing

use crate::{Error, OtlpMmapConfig};
use memmap2::{MmapMut, MmapOptions};
use std::{
    sync::atomic::{AtomicI64, AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

/// Current supported versions of OTLP-MMAP from this crate.
const SUPPORTED_MMAP_VERSION: &[i64] = &[1];
/// Current MMAP version for files we create.
const CURRENT_MMAP_VERSION: i64 = 1;
const RING_BUFFER_HEADER_SIZE: usize = 32;

/// Determine the minimum file size needed for a given config
pub(crate) fn calculate_minimum_file_size(config: &OtlpMmapConfig) -> u64 {
    // Start with header size
    64 + ring_buffer_size(config.events.num_buffers, config.events.buffer_size) as u64
        + ring_buffer_size(config.spans.num_buffers, config.spans.buffer_size) as u64
        + ring_buffer_size(
            config.measurements.num_buffers,
            config.measurements.buffer_size,
        ) as u64
        + 64
        + config.dictionary.initial_size
}

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
    fn raw_mut(&mut self) -> &mut RawMmapHeader {
        unsafe { &mut *(self.data.as_ref().as_ptr() as *mut RawMmapHeader) }
    }

    /// Checks whether the version in the header is one we support.
    pub fn check_version(&self) -> Result<(), Error> {
        if !SUPPORTED_MMAP_VERSION.contains(&self.version()) {
            return Err(Error::VersionMismatch(
                self.version(),
                SUPPORTED_MMAP_VERSION,
            ));
        }
        Ok(())
    }

    /// Initialize this header for writing.
    ///
    /// TODO - We need configuration input on sizes.
    pub fn initialize(&mut self, config: &OtlpMmapConfig) -> Result<(), Error> {
        // TODO - version should use an atomic as well.
        self.raw_mut().version = CURRENT_MMAP_VERSION;
        let start_time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as u64;
        self.raw()
            .start_time_unix_nano
            .store(start_time, Ordering::Release);
        // Calculate and write ring buffer / dictionary offsets.
        let mut offset = 64; // File header size.
        let event_offset = offset as i64;
        self.raw().events.store(event_offset, Ordering::Release);
        offset += ring_buffer_size(config.events.num_buffers, config.events.buffer_size);
        let span_offset = offset as i64;
        self.raw().spans.store(span_offset, Ordering::Release);
        offset += ring_buffer_size(config.spans.num_buffers, config.spans.buffer_size);
        let measurement_offset = offset as i64;
        self.raw()
            .measurements
            .store(measurement_offset, Ordering::Release);
        offset += ring_buffer_size(
            config.measurements.num_buffers,
            config.measurements.buffer_size,
        );
        self.raw()
            .dictionary
            .store(offset as i64, Ordering::Release);
        // Note - This does NOT initialize the ringbuffers or dictionary header, they will need to that on their own.
        Ok(())
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

/// Calculate the size a ringbuffer will take up in an OTLP-MMAP file.
fn ring_buffer_size(num_buffers: usize, buffer_size: usize) -> usize {
    // Header + Availability + Buffers
    RING_BUFFER_HEADER_SIZE + (4 * num_buffers) + (num_buffers * buffer_size)
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
