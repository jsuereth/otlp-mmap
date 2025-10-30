//! SDK MMap file reading components.

use std::{
    fs::OpenOptions,
    path::Path,
    sync::atomic::{AtomicI64, Ordering},
};

use memmap::MmapOptions;

pub mod data;
pub mod dictionary;
pub mod ringbuffer;

use data::{Event, Measurement, SpanEvent};
use ringbuffer::RingBufferReader;

use crate::{oltp_mmap::Error, sdk_mmap::dictionary::Dictionary};

pub struct MmapReader {
    pub events: RingBufferReader<Event>,
    pub spans: RingBufferReader<SpanEvent>,
    pub metrics: RingBufferReader<Measurement>,
    pub dictionary: Dictionary,
}

impl MmapReader {
    pub fn new(path: &Path) -> Result<MmapReader, Error> {
        println!("Reading file [{}]", path.display());
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(path)?;
        let raw_header = unsafe {
            MmapOptions::new()
            .offset(0)
            .len(64)
            .map_mut(&f)?
        };
        println!("Opened header {:?}", raw_header);
        let header = unsafe {
            &*(raw_header.as_ref().as_ptr() as *const MmapHeader)
        };
        println!("Reading header, expected size {} ", std::mem::size_of::<MmapHeader>());
        // This is the order of blocks in the file.
        // We use this to load separate MMap instances for the various sections.
        println!("version: {}", header.version);
        let event_start = header.events.load(Ordering::Relaxed);
        let span_start = header.spans.load(Ordering::Relaxed);
        let measurement_start = header.measurements.load(Ordering::Relaxed);
        let dictionary_start = header.dictionary.load(Ordering::Relaxed);
        eprintln!("Found header: ");
        eprintln!("events: {event_start}");
        eprintln!("spans: {span_start}");
        eprintln!("measurements: {measurement_start}");
        eprintln!("dictionary: {dictionary_start}");
        let events: RingBufferReader<Event> = unsafe {
            let event_area = MmapOptions::new()
                .len((span_start - event_start) as usize)
                .offset(event_start as u64)
                .map_mut(&f)?;
            RingBufferReader::new(event_area, 0)
        };
        println!("Loaded events");
        let spans: RingBufferReader<SpanEvent> = unsafe {
            let event_area = MmapOptions::new()
                .len((measurement_start - span_start) as usize)
                .offset(span_start as u64)
                .map_mut(&f)?;
            RingBufferReader::new(event_area, 0)
        };
        println!("Loaded spans");
        let metrics: RingBufferReader<Measurement> = unsafe {
            let event_area = MmapOptions::new()
                .len((dictionary_start - measurement_start) as usize)
                .offset(measurement_start as u64)
                .map_mut(&f)?;
            RingBufferReader::new(event_area, 0)
        };
        println!("Loaded metrics");
        let dictionary = unsafe {
            let dictionary_area = MmapOptions::new()
            .offset(dictionary_start as u64)
            .map_mut(&f)?;
            Dictionary::new(dictionary_area, 0)
        };
        println!("Loaded dictionary");
        Ok(MmapReader {
            events,
            spans,
            metrics,
            dictionary,
        })
    }
}

#[repr(C)]
struct MmapHeader {
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
}
