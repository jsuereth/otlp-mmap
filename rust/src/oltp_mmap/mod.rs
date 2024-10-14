use std::path::Path;
use dictionary::DictionaryInputChannel;
use opentelemetry_proto::tonic::{common::v1::InstrumentationScope, resource::v1::Resource, trace::v1::Span};
use prost::Message;
use ringbuffer::RingbufferInputChannel;
use span_ref::SpanRef;


pub mod ringbuffer;
pub mod dictionary;
pub mod span_ref;
pub mod r#async;

/// An implementation that reads OTLP data.
pub struct OtlpInputCommon {
    resources: DictionaryInputChannel,
    scopes: DictionaryInputChannel,
    spans: RingbufferInputChannel
}

impl OtlpInputCommon {
    pub fn new(p: &Path) -> OtlpInputCommon {
        let resources = DictionaryInputChannel::new(&p.join("resource.otlp"));
        let scopes = DictionaryInputChannel::new(&p.join("scope.otlp"));
        let spans = RingbufferInputChannel::new(&p.join("spans.otlp"));
        OtlpInputCommon { resources,scopes,spans}
    }

    // Returns true if all OTLP files still refer to the same version.
    pub fn is_sane(&self) -> bool {
        self.resources.version() == self.scopes.version() &&
        self.resources.version() == self.spans.version()
    }
    /// Reads the resource referenced by an index.
    pub fn resource(&self, idx: i64) -> Option<Resource> {
        self.resources.entry(idx).and_then(|buf| read_resource(&buf))
    }
    /// Reads the instrumentation scope referenced by an index.
    pub fn scope(&self, idx: i64) -> Option<InstrumentationScope> {
        self.scopes.entry(idx).and_then(|buf| read_scope(&buf))
    }

    // TODO - error handling.
    /// Polls until the next span to read is available.
    pub fn next_span(&mut self) -> Option<OtlpSpan> {
         // TODO - every now and then check sanity before continuing...?
         read_span_ref(&self.spans.next())
    }
}

/// Actually reads the resource from chunks.
fn read_resource(buf: &[u8]) -> Option<Resource> {
    match Resource::decode_length_delimited(buf) {
        Ok(resource) => Some(resource),
        // TODO - Save errors.
        Err(_) => None,
    }
}

/// Actually reads the scope from chunks.
fn read_scope(buf: &[u8]) -> Option<InstrumentationScope> {
    match InstrumentationScope::decode_length_delimited(buf) {
        Ok(scope) => Some(scope),
        // TODO - Save errors.
        Err(_) => None,
    }
}

/// Actually reads the span references from chunks.
fn read_span_ref(buf: &[u8]) -> Option<OtlpSpan> {
    match SpanRef::decode_length_delimited(buf) {
        Ok(span_ref) => Some(OtlpSpan {
            // span: span_ref.span,
            resource: span_ref.resource_ref,
            scope: span_ref.scope_ref,
            span: span_ref.span,
        }),
        // TODO - Save errors.
        Err(_) => None,
    }
}

/// A span sent via MMAP OTLP where resource/scope are sent by index.
pub struct OtlpSpan {
    // pub span: Span,
    pub resource: i64,
    pub scope: i64,
    pub span: Span,
}

impl std::fmt::Display for OtlpSpan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "resource: {}, scope: {}, span: {}", self.resource, self.scope, self.span.name)
    }
}