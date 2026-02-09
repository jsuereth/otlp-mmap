//! OTLP-MMAP Core processing/utilities for interacting with these files.

mod dictionary;
mod error;
mod ringbuffer;

pub use dictionary::Dictionary;
pub use error::Error;
use otlp_mmap_protocol::{Event, Measurement, SpanEvent};
// Exposes the various ringbuffer APIs we need.
pub use ringbuffer::{RingBuffer, RingBufferReader, RingBufferWriter};

/// A reader of OTLP-MMAP files.
pub trait OtlpMmapReader {
    // TODO - sanity checking function.

    /// Ring of events coming in.
    fn events(&self) -> &impl RingBufferReader<Event>;
    /// Ring of span events coming in.
    fn spans(&self) -> &impl RingBufferReader<SpanEvent>;
    /// Ring of measurements coming in.
    fn metrics(&self) -> &impl RingBufferReader<Measurement>;
    // TODO - fancier dictionary abilities.
    /// Dictionary of things we need to lookup.
    fn dictionary(&self) -> &Dictionary;
}

/// A writer of OTLP-MMAP files.
pub trait OtlpMmapWriter {
    /// Ring of events to write into.
    fn events(&mut self) -> &mut impl RingBufferWriter<Event>;
    /// Ring of events to write into.
    fn spans(&mut self) -> &mut impl RingBufferWriter<SpanEvent>;
    /// Ring of events to write into.
    fn metrics(&mut self) -> &mut impl RingBufferWriter<Measurement>;
    // TODO - fancier dictionary abilities.
    /// Dictionary of things we need to compress.
    fn dictionary(&mut self) -> &mut Dictionary;
}

struct OtlpMmapFile {}

// TODO - define our public API here.
