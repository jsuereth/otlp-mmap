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
    #[prost(bytes = "vec", tag = 1)]
    trace_id: ::prost::alloc::vec::Vec<u8>,
    /// Unique id for trace.
    #[prost(bytes = "vec", tag = 2)]
    span_id: ::prost::alloc::vec::Vec<u8>,

    #[prost(oneof = "SpanEventEnum", tags = "11, 12")]
    #[serde(flatten)]
    pub value: ::core::option::Option<SpanEventEnum>
}

#[derive(Clone, PartialEq, ::prost::Message, serde::Serialize, serde::Deserialize)]
pub struct SpanEventEvent {
    #[prost(oneof = "SpanEventEnum", tags = "11, 12, 13, 14, 15")]
    #[serde(flatten)]
    pub value: ::core::option::Option<SpanEventEnum>
}
#[derive(Clone, PartialEq, ::prost::Oneof, serde::Serialize, serde::Deserialize)]
pub enum SpanEventEnum {
    #[prost(message, tag = "11")]
    Start(StartSpan),
    #[prost(message, tag = "12")]
    End(EndSpan)
}

#[derive(Clone, PartialEq, ::prost::Message, serde::Serialize, serde::Deserialize)]
pub struct StartSpan {
    #[prost(string, tag = "5")]
    pub name: String,
    // time_unix_nano is the time when the event occurred.
    #[prost(fixed64, tag = 7)]
    start_time_unix_nano: u64,
}

#[derive(Clone, PartialEq, ::prost::Message, serde::Serialize, serde::Deserialize)]
pub struct EndSpan {
    // time_unix_nano is the time when the event occurred.
    #[prost(fixed64, tag = 8)]
    end_time_unix_nano: u64,
}

/// Metric Events sent via ringbuffer.
#[derive(Clone, PartialEq, ::prost::Message, serde::Serialize, serde::Deserialize)]
pub struct Measurement {
    #[prost(int64, tag = 1)]
    metric_ref: i64,
    #[prost(fixed64, tag = 3)]
    time_unix_nano: u64,
}
