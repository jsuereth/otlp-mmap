//! SDK MMap file reading components.

use std::{
    fs::OpenOptions,
    path::Path,
    sync::atomic::{AtomicI64, AtomicU64, Ordering},
};

use crate::sdk_mmap::data::{Event, Measurement, SpanEvent};
use crate::sdk_mmap::ringbuffer::RingBufferReader;
use memmap2::{MmapMut, MmapOptions};

use crate::{sdk_mmap::dictionary::Dictionary, sdk_mmap::Error};

const SUPPORTED_MMAP_VERSION: &[i64] = &[1];

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

    /// Start time when we created this MmapReader.
    pub fn start_time(&self) -> u64 {
        self.start_time
    }

    /// Checks if the start time of the MMap file is the same.
    /// If not, the MMAP file was likely restarted.
    pub fn has_file_changed(&self) -> bool {
        self.start_time != self.header.start_time()
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
