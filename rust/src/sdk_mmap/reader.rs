//! SDK MMap file reading components.

use std::{
    fs::OpenOptions,
    path::Path,
    sync::atomic::{AtomicI64, Ordering},
};

use crate::sdk_mmap::data::{Event, Measurement, SpanEvent};
use crate::sdk_mmap::ringbuffer::RingBufferReader;
use memmap2::MmapOptions;

use crate::{oltp_mmap::Error, sdk_mmap::dictionary::Dictionary};

/// Raw reader of mmap files.
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
            .create(true)
            .open(path)?;
        let raw_header = unsafe { MmapOptions::new().offset(0).len(64).map_mut(&f)? };
        let header = unsafe { &*(raw_header.as_ref().as_ptr() as *const MmapHeader) };
        // This is the order of blocks in the file.
        // We use this to load separate MMap instances for the various sections.
        let event_start = header.events.load(Ordering::Relaxed);
        let span_start = header.spans.load(Ordering::Relaxed);
        let measurement_start = header.measurements.load(Ordering::Relaxed);
        let dictionary_start = header.dictionary.load(Ordering::Relaxed);
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
        // Dictionary may need to remap itself.
        let dictionary = Dictionary::try_new(f, dictionary_start as u64)?;
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
