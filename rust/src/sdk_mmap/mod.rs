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
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(path)?;
        let header = unsafe {
            let raw_header = MmapOptions::new().len(64).map_mut(&f)?;
            &*(raw_header.as_ptr() as *const &MmapHeader)
        };
        // This is the order of blocks in the file.
        // We use this to load separate MMap instances for the various sections.
        let event_start = header.events.load(Ordering::Acquire);
        let span_start = header.spans.load(Ordering::Acquire);
        let measurement_start = header.spans.load(Ordering::Acquire);
        let dictionary_start = header.dictionary.load(Ordering::Acquire);

        let events: RingBufferReader<Event> = unsafe {
            let event_area = MmapOptions::new()
                .len((span_start - event_start) as usize)
                .offset(event_start as u64)
                .map_mut(&f)?;
            RingBufferReader::new(event_area, 0)
        };
        let spans: RingBufferReader<SpanEvent> = unsafe {
            let event_area = MmapOptions::new()
                .len((measurement_start - span_start) as usize)
                .offset(span_start as u64)
                .map_mut(&f)?;
            RingBufferReader::new(event_area, 0)
        };
        let metrics: RingBufferReader<Measurement> = unsafe {
            let event_area = MmapOptions::new()
                .len((dictionary_start - measurement_start) as usize)
                .offset(measurement_start as u64)
                .map_mut(&f)?;
            RingBufferReader::new(event_area, 0)
        };
        let dictionary = unsafe {
            let dictionary_area = MmapOptions::new()
            .offset(dictionary_start as u64)
            .map_mut(&f)?;
            Dictionary::new(dictionary_area, 0)
        };
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
    version: AtomicI64,
    /// Location of logs event buffer.
    events: AtomicI64,
    /// Location of spans event buffer.
    spans: AtomicI64,
    /// Location of measurements event buffer.
    measurements: AtomicI64,
    /// Location of dictionary.
    dictionary: AtomicI64,
}
