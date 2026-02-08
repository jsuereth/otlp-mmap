//! SDK MMap file reading components.

use std::{
    fs::OpenOptions,
    sync::atomic::{AtomicI64, AtomicU64, Ordering},
};

use crate::sdk_mmap::ringbuffer::RingBufferReader;
use crate::sdk_mmap::{
    data::{Event, Measurement, SpanEvent},
    dictionary::Dictionary,
};
use memmap2::{MmapMut, MmapOptions};

use super::Error;

const SUPPORTED_MMAP_VERSION: &[i64] = &[1];

/// Raw reader of mmap files.
pub struct MmapReader {
    pub header: MmapHeader,
    pub events: RingBufferReader<Event>,
    pub spans: RingBufferReader<SpanEvent>,
    pub metrics: RingBufferReader<Measurement>,
    pub dictionary: Dictionary,
    #[allow(dead_code)]
    start_time: u64,
}

impl MmapReader {
    pub fn new(path: &str) -> Result<MmapReader, Error> {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(path)?;
        let header = MmapHeader::new(&f)?;
        if !SUPPORTED_MMAP_VERSION.contains(&header.version()) {
            return Err(Error::VersionMismatch(
                header.version(),
                SUPPORTED_MMAP_VERSION,
            ));
        }
        let start_time = header.start_time();
        
        let event_start = header.events_offset();
        let span_start = header.spans_offset();
        let measurement_start = header.measurements_offset();
        let dictionary_start = header.dictionary_offset();
        
        let events: RingBufferReader<Event> = unsafe {
            let event_area = MmapOptions::new()
                .len((span_start - event_start) as usize)
                .offset(event_start as u64)
                .map_mut(&f)?;
            RingBufferReader::new(event_area, 0)
        };
        let spans: RingBufferReader<SpanEvent> = unsafe {
            let span_area = MmapOptions::new()
                .len((measurement_start - span_start) as usize)
                .offset(span_start as u64)
                .map_mut(&f)?;
            RingBufferReader::new(span_area, 0)
        };
        let metrics: RingBufferReader<Measurement> = unsafe {
            let measurement_area = MmapOptions::new()
                .len((dictionary_start - measurement_start) as usize)
                .offset(measurement_start as u64)
                .map_mut(&f)?;
            RingBufferReader::new(measurement_area, 0)
        };
        
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
    
    pub fn read_event(&self) -> Result<Option<Event>, Error> {
        self.events.try_read_next()
    }
    
    pub fn read_span(&self) -> Result<Option<SpanEvent>, Error> {
        self.spans.try_read_next()
    }
    
    pub fn read_metric(&self) -> Result<Option<Measurement>, Error> {
        self.metrics.try_read_next()
    }
    
    pub fn dictionary(&self) -> &Dictionary {
        &self.dictionary
    }
}

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

    pub fn version(&self) -> i64 {
        self.raw().version
    }
    pub fn start_time(&self) -> u64 {
        self.raw().start_time_unix_nano.load(Ordering::Acquire)
    }
    pub fn events_offset(&self) -> i64 {
        self.raw().events.load(Ordering::Relaxed)
    }
    pub fn spans_offset(&self) -> i64 {
        self.raw().spans.load(Ordering::Relaxed)
    }
    pub fn measurements_offset(&self) -> i64 {
        self.raw().measurements.load(Ordering::Relaxed)
    }
    pub fn dictionary_offset(&self) -> i64 {
        self.raw().dictionary.load(Ordering::Relaxed)
    }
}

#[repr(C)]
struct RawMmapHeader {
    version: i64,
    events: AtomicI64,
    spans: AtomicI64,
    measurements: AtomicI64,
    dictionary: AtomicI64,
    start_time_unix_nano: AtomicU64,
}
