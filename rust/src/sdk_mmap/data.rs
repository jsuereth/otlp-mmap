//! OTLP MMap definitions.

/// Log Events sent via ringbuffer.
#[derive(Clone, PartialEq, ::prost::Message, serde::Serialize, serde::Deserialize)]
pub struct Event {
    /// InsturmentationScope from which this was recorded.
    #[prost(int64, tag = 1)]
    scope_ref: i64,
    // time_unix_nano is the time when the event occurred.
    #[prost(fixed64, tag = 2)]
    time_unix_nano: u64,
    // TODO - other aspects.
}

/// Span Events sent via ringbuffer.
#[derive(Clone, PartialEq, ::prost::Message, serde::Serialize, serde::Deserialize)]
pub struct SpanEvent {
    /// Unique id for trace.
    #[prost(bytes, tag = 1)]
    trace_id: Vec<u8>,
    /// Unique id for trace.
    #[prost(bytes, tag = 2)]
    span_id: Vec<u8>,
}

/// Metric Events sent via ringbuffer.
#[derive(Clone, PartialEq, ::prost::Message, serde::Serialize, serde::Deserialize)]
pub struct Measurement {
    #[prost(int64, tag = 1)]
    metric_ref: i64,
    #[prost(fixed64, tag = 3)]
    time_unix_nano: u64,
}
