//! OTLP-MMAP Core processing/utilities for interacting with these files.

mod convert;
mod dictionary;
mod error;
mod header;
mod ringbuffer;

use std::{fs::OpenOptions, path::Path};

pub use convert::OtlpDictionary;
use dictionary::Dictionary;
pub use error::Error;
use header::MmapHeader;
use memmap2::MmapOptions;
use otlp_mmap_protocol::{Event, Measurement, SpanEvent};
// Exposes the various ringbuffer APIs we need.
pub use ringbuffer::{RingBufferReader, RingBufferWriter};

// TODO - Refactor this like OTLP-MMAP reader was refactored.
/// A writer of OTLP-MMAP files.
pub trait OtlpMmapWriter {
    /// Ring of events to write into.
    fn events(&mut self) -> &mut RingBufferWriter<Event>;
    /// Ring of events to write into.
    fn spans(&mut self) -> &mut RingBufferWriter<SpanEvent>;
    /// Ring of events to write into.
    fn metrics(&mut self) -> &mut RingBufferWriter<Measurement>;
    // TODO - fancier dictionary abilities.
    /// Dictionary of things we need to compress.
    fn dictionary(&mut self) -> &mut Dictionary;
}

const SUPPORTED_MMAP_VERSION: &[i64] = &[1];
pub struct OtlpMmapReader {
    header: MmapHeader,
    events: RingBufferReader<Event>,
    spans: RingBufferReader<SpanEvent>,
    metrics: RingBufferReader<Measurement>,
    dictionary: OtlpDictionary,
    start_time: u64,
}

impl OtlpMmapReader {
    /// Constructs a new OTLP-MMAP File handler at the given location.
    pub fn new(path: &Path) -> Result<OtlpMmapReader, Error> {
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
        let events = unsafe {
            let event_area = MmapOptions::new()
                .len((span_start - event_start) as usize)
                .offset(event_start as u64)
                .map_mut(&f)?;
            RingBufferReader::<Event>::new(event_area, 0)
        };
        println!("Loading span channel @ {span_start}");
        let spans = unsafe {
            let span_area = MmapOptions::new()
                .len((measurement_start - span_start) as usize)
                .offset(span_start as u64)
                .map_mut(&f)?;
            RingBufferReader::<SpanEvent>::new(span_area, 0)
        };
        println!("Loading measurment channel @ {measurement_start}");
        let metrics = unsafe {
            let measurement_area = MmapOptions::new()
                .len((dictionary_start - measurement_start) as usize)
                .offset(measurement_start as u64)
                .map_mut(&f)?;
            RingBufferReader::<Measurement>::new(measurement_area, 0)
        };
        println!("Loading dictionary @ {dictionary_start}");
        // Dictionary may need to remap itself.
        let dictionary = OtlpDictionary::new(Dictionary::try_new(f, dictionary_start as u64)?);
        Ok(OtlpMmapReader {
            header,
            events,
            spans,
            metrics,
            dictionary,
            start_time,
        })
    }

    /// Ring of events coming in.
    pub fn events(&self) -> &RingBufferReader<Event> {
        &self.events
    }
    /// Ring of span events coming in.
    pub fn spans(&self) -> &RingBufferReader<SpanEvent> {
        &self.spans
    }
    /// Ring of measurements coming in.
    pub fn metrics(&self) -> &RingBufferReader<Measurement> {
        &self.metrics
    }
    /// Dictionary for looking up things.
    pub fn dictionary(&self) -> &OtlpDictionary {
        &self.dictionary
    }

    /// Returns true if we detect the file has changed behind us.
    pub fn has_file_changed(&self) -> bool {
        self.start_time != self.header.start_time()
    }

    /// The start time of the MMAP file in nanoseconds since epoch.
    pub fn start_time(&self) -> u64 {
        self.start_time
    }
}
