//! OTLP-MMAP Core processing/utilities for interacting with these files.

mod config;
mod convert;
mod dictionary;
mod error;
mod header;
mod ringbuffer;

use std::{fs::OpenOptions, path::Path};

// Exposes the various ringbuffer APIs we need.
pub use ringbuffer::{RingBufferReader, RingBufferWriter};
// Exposes the high level dictionary reader we need.
pub use convert::OtlpDictionary;
// Exposes the configuration used for reading/writing.
pub use config::{DictionaryConfig, OtlpMmapConfig, RingBufferConfig};
// Exposes the error handling we use.
pub use error::Error;

use dictionary::Dictionary;
use header::MmapHeader;
use memmap2::MmapOptions;
use otlp_mmap_protocol::{Event, Measurement, SpanEvent};

/// A very low-level writer of OTLP-MMAP files.
pub struct OtlpMmapWriter {
    header: MmapHeader,
    events: RingBufferWriter<Event>,
    spans: RingBufferWriter<SpanEvent>,
    metrics: RingBufferWriter<Measurement>,
    dictionary: Dictionary,
}

impl OtlpMmapWriter {
    /// Constructs a new OTLP-MMAP writer with the given config.
    pub fn new(path: &Path, config: &OtlpMmapConfig) -> Result<OtlpMmapWriter, Error> {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let mut header = MmapHeader::new(&f)?;
        header.initialize(config)?;
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
            RingBufferWriter::<Event>::new(
                event_area,
                0,
                config.events.buffer_size,
                config.events.num_buffers,
            )
        };
        println!("Loading span channel @ {span_start}");
        let spans = unsafe {
            let span_area = MmapOptions::new()
                .len((measurement_start - span_start) as usize)
                .offset(span_start as u64)
                .map_mut(&f)?;
            RingBufferWriter::<SpanEvent>::new(
                span_area,
                0,
                config.spans.buffer_size,
                config.spans.num_buffers,
            )
        };
        println!("Loading measurment channel @ {measurement_start}");
        let metrics = unsafe {
            let measurement_area = MmapOptions::new()
                .len((dictionary_start - measurement_start) as usize)
                .offset(measurement_start as u64)
                .map_mut(&f)?;
            RingBufferWriter::<Measurement>::new(
                measurement_area,
                0,
                config.measurements.buffer_size,
                config.measurements.num_buffers,
            )
        };
        println!("Loading dictionary @ {dictionary_start}");
        // Dictionary may need to remap itself.
        let dictionary = Dictionary::try_new(
            f,
            dictionary_start as u64,
            Some(config.dictionary.initial_size),
        )?;
        Ok(OtlpMmapWriter {
            header,
            events,
            spans,
            metrics,
            dictionary,
        })
    }
}

/// A very low-level reader of OTLP-MMAP files.
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
        header.check_version()?;
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
        let dictionary =
            OtlpDictionary::new(Dictionary::try_new(f, dictionary_start as u64, None)?);
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
