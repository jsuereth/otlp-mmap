//! SDK MMap file reading components.

use std::{
    fs::OpenOptions,
    path::Path,
    sync::atomic::{AtomicI64, AtomicU64, Ordering},
};

use crate::sdk_mmap::ringbuffer::RingBufferReader;
use crate::sdk_mmap::{
    data::{Event, Measurement, SpanEvent},
    dictionary::AsyncDictionary,
    ringbuffer::AsyncEventQueue,
};
use memmap2::{MmapMut, MmapOptions};

use crate::{sdk_mmap::dictionary::Dictionary, sdk_mmap::Error};

const SUPPORTED_MMAP_VERSION: &[i64] = &[1];

/// Trait used to stub out the behavior of reading MMap files
pub trait AsyncMmapReader {
    /// The queue for reading spans.
    fn spans_queue<'a>(&'a self) -> &'a (impl AsyncEventQueue<SpanEvent> + Sync);
    /// The queue for reading measurements.
    fn measurement_queue<'a>(&'a self) -> &'a impl AsyncEventQueue<Measurement>;
    /// The queue for reading events.
    fn event_queue<'a>(&'a self) -> &'a impl AsyncEventQueue<Event>;
    /// The queue for reading dictionary entries.
    fn dictionary<'a>(&'a self) -> &'a (impl AsyncDictionary + Sync);
    /// Start time when we created this MmapReader.
    fn start_time(&self) -> u64;
    /// Checks if the start time of the MMap file is the same.
    /// If not, the MMAP file was likely restarted.
    fn has_file_changed(&self) -> bool;
}

impl AsyncMmapReader for MmapReader {
    fn spans_queue<'a>(&'a self) -> &'a impl AsyncEventQueue<SpanEvent> {
        &self.spans
    }

    fn measurement_queue<'a>(&'a self) -> &'a impl AsyncEventQueue<Measurement> {
        &self.metrics
    }

    fn event_queue<'a>(&'a self) -> &'a impl AsyncEventQueue<Event> {
        &self.events
    }

    fn dictionary<'a>(&'a self) -> &'a impl AsyncDictionary {
        &self.dictionary
    }

    fn start_time(&self) -> u64 {
        self.start_time
    }

    fn has_file_changed(&self) -> bool {
        self.start_time != self.header.start_time()
    }
}

/// Raw reader of mmap files.
pub struct MmapReader {
    /// Header of the mmap file.
    pub header: MmapHeader,
    /// Ringbuffer where events will be sent.
    pub events: RingBufferReader<Event>,
    /// Ring buffer where span events are sent.
    pub spans: RingBufferReader<SpanEvent>,
    /// Ring bfufer where metric measurements are sent.
    pub metrics: RingBufferReader<Measurement>,
    /// Dictionary of shared values.
    pub dictionary: Dictionary,
    // TODO - Should we keep the header around so we can check sanity?
    start_time: u64,
}

impl MmapReader {
    pub fn new(path: &Path) -> Result<MmapReader, Error> {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let header = MmapHeader::new(&f)?;
        if !SUPPORTED_MMAP_VERSION.contains(&header.version()) {
            return Err(Error::VersionMismatch(
                header.version(),
                SUPPORTED_MMAP_VERSION,
            ));
        }
        let start_time = header.start_time();
        // This is the order of blocks in the file.
        // We use this to load separate MMap instances for the various sections.
        let event_start = header.events_offset();
        let span_start = header.spans_offset();
        let measurement_start = header.measurements_offset();
        let dictionary_start = header.dictionary_offset();
        println!("Loading log channel @ {event_start}");
        let events: RingBufferReader<Event> = unsafe {
            let event_area = MmapOptions::new()
                .len((span_start - event_start) as usize)
                .offset(event_start as u64)
                .map_mut(&f)?;
            RingBufferReader::new(event_area, 0)
        };
        println!("Loading span channel @ {span_start}");
        let spans: RingBufferReader<SpanEvent> = unsafe {
            let span_area = MmapOptions::new()
                .len((measurement_start - span_start) as usize)
                .offset(span_start as u64)
                .map_mut(&f)?;
            RingBufferReader::new(span_area, 0)
        };
        println!("Loading measurment channel @ {measurement_start}");
        let metrics: RingBufferReader<Measurement> = unsafe {
            let measurement_area = MmapOptions::new()
                .len((dictionary_start - measurement_start) as usize)
                .offset(measurement_start as u64)
                .map_mut(&f)?;
            RingBufferReader::new(measurement_area, 0)
        };
        println!("Loading dictionary @ {dictionary_start}");
        // Dictionary may need to remap itself.
        let dictionary = Dictionary::try_new(f, dictionary_start as u64)?;
        Ok(MmapReader {
            header,
            events,
            spans,
            metrics,
            dictionary,
            start_time,
        })
    }
}

/// Header of the MMap File.  We use this to check sanity / change of the overall file.
pub struct MmapHeader {
    data: MmapMut,
}

impl MmapHeader {
    fn new<F>(file: F) -> Result<MmapHeader, Error>
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
    use std::fs::File;
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

    /// Helper to write a ring buffer header at a specific offset.
    fn write_rb_header(
        file: &mut File,
        offset: u64,
        num_buffers: i64,
        buffer_size: i64,
    ) -> std::io::Result<()> {
        file.seek(std::io::SeekFrom::Start(offset))?;
        file.write_all(&num_buffers.to_ne_bytes())?;
        file.write_all(&buffer_size.to_ne_bytes())?;
        file.write_all(&(-1i64).to_ne_bytes())?; // reader_index
        file.write_all(&(-1i64).to_ne_bytes())?; // writer_index
        file.flush()
    }

    /// Sets up a complete and valid MMAP file for testing.
    fn setup_test_mmap_file(file: &mut File) -> std::io::Result<()> {
        let events_offset = HEADER_SIZE;
        let spans_offset = events_offset + 1024;
        let measurements_offset = spans_offset + 1024;
        let dictionary_offset = measurements_offset + 1024;
        let total_size = dictionary_offset + 1024;

        file.set_len(total_size)?;

        // Write main header
        write_main_header(
            file,
            1,
            events_offset as i64,
            spans_offset as i64,
            measurements_offset as i64,
            dictionary_offset as i64,
            12345,
        )?;

        // Write headers for each ring buffer
        write_rb_header(file, events_offset, 8, 128)?;
        write_rb_header(file, spans_offset, 8, 128)?;
        write_rb_header(file, measurements_offset, 8, 128)?;

        Ok(())
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

    #[test]
    fn test_mmap_reader_new_success() {
        let file = NamedTempFile::new().unwrap();
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())
            .unwrap();
        setup_test_mmap_file(&mut f).unwrap();

        // This should not panic or return an error
        let reader_result = MmapReader::new(file.path());
        assert!(reader_result.is_ok());
    }

    #[test]
    fn test_mmap_reader_new_version_mismatch() {
        let file = NamedTempFile::new().unwrap();
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())
            .unwrap();
        f.set_len(1024).unwrap();
        write_main_header(&mut f, 99, HEADER_SIZE as i64, 200, 300, 400, 0).unwrap();
        write_rb_header(&mut f, HEADER_SIZE, 8, 128).unwrap();

        let reader_result = MmapReader::new(file.path());
        assert!(matches!(reader_result, Err(Error::VersionMismatch(99, _))));
    }

    #[test]
    fn test_mmap_reader_new_truncated_header() {
        let file = NamedTempFile::new().unwrap();
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())
            .unwrap();
        // File is smaller than the header size
        f.set_len(32).unwrap();

        // MmapHeader::new should fail, which MmapReader::new propagates
        let reader_result = MmapReader::new(file.path());
        assert!(matches!(reader_result, Err(Error::IoError(_))));
    }

    #[test]
    fn test_mmap_reader_new_invalid_offsets_errors() {
        let file = NamedTempFile::new().unwrap();
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())
            .unwrap();
        f.set_len(1024).unwrap();

        // Spans offset is smaller than events offset, which will cause an error
        // when trying to create a mapping with a negative length.
        write_main_header(&mut f, 1, 200, 100, 300, 400, 12345).unwrap();
        write_rb_header(&mut f, 200, 8, 128).unwrap();

        let result = MmapReader::new(file.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_mmap_reader_has_file_changed() -> Result<(), Error> {
        let file = NamedTempFile::new()?;
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())?;
        setup_test_mmap_file(&mut f)?;

        let reader = MmapReader::new(file.path())?;
        assert!(!reader.has_file_changed());

        // Now, change the start time in the file header
        let current_offsets = (
            reader.header.events_offset(),
            reader.header.spans_offset(),
            reader.header.measurements_offset(),
            reader.header.dictionary_offset(),
        );
        let new_time = 67890;
        write_main_header(
            &mut f,
            1,
            current_offsets.0,
            current_offsets.1,
            current_offsets.2,
            current_offsets.3,
            new_time,
        )?;

        assert!(reader.has_file_changed());
        Ok(())
    }
}
