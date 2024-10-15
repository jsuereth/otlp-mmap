use opentelemetry_proto::tonic::trace::v1::Span;

/// SpanRef - The proto message format used for the spans.otlp ringbuffer.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message, serde::Serialize, serde::Deserialize)]
pub struct SpanRef {
    /// Index reference to the resource this span uses.
    #[prost(int64, tag = 1)]
    pub resource_ref: i64,
    /// Index Reference to the scope this span uses.
    #[prost(int64, tag = 2)]
    pub scope_ref: i64,
    /// The span being sent.
    #[prost(message, required, tag = 3)]
    pub span: Span,
}
