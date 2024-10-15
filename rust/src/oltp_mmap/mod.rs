use dictionary::DictionaryInputChannel;
use error::OltpMmapError;
use opentelemetry_proto::tonic::{
    common::v1::InstrumentationScope, resource::v1::Resource, trace::v1::Span,
};
use prost::Message;
use ringbuffer::RingbufferInputChannel;
use span_ref::SpanRef;
use std::path::Path;

type Error = error::OltpMmapError;

pub mod dictionary;
pub mod error;
pub mod ringbuffer;
pub mod span_ref;
// pub mod r#async;

/// An implementation that reads OTLP data.
pub struct OtlpInputCommon {
    resources: DictionaryInputChannel,
    scopes: DictionaryInputChannel,
    spans: RingbufferInputChannel,
}

impl OtlpInputCommon {
    pub fn new(p: &Path) -> Result<OtlpInputCommon, Error> {
        let resources = DictionaryInputChannel::new(&p.join("resource.otlp"))?;
        let scopes = DictionaryInputChannel::new(&p.join("scope.otlp"))?;
        let spans = RingbufferInputChannel::new(&p.join("spans.otlp"))?;
        Ok(OtlpInputCommon {
            resources,
            scopes,
            spans,
        })
    }

    // Returns true if all OTLP files still refer to the same version.
    pub fn is_sane(&self) -> bool {
        self.resources.version() == self.scopes.version()
            && self.resources.version() == self.spans.version()
    }
    /// Reads the resource referenced by an index.
    pub fn resource(&self, idx: i64) -> Result<Resource, Error> {
        let buf = self.resources.entry(idx)?;
        read_resource(&buf)
    }
    /// Reads the instrumentation scope referenced by an index.
    pub fn scope(&self, idx: i64) -> Result<InstrumentationScope, Error> {
        let buf = self.scopes.entry(idx)?;
        read_scope(&buf)
    }

    // TODO - error handling.
    /// Polls until the next span to read is available.
    pub fn next_span(&mut self) -> Result<OtlpSpan, Error> {
        // TODO - every now and then check sanity before continuing...?
        read_span_ref(&self.spans.next())
    }
}

/// Actually reads the resource from chunks.
fn read_resource(buf: &[u8]) -> Result<Resource, Error> {
    Resource::decode_length_delimited(buf).map_err(OltpMmapError::ProtobufDecodeError)
}

/// Actually reads the scope from chunks.
fn read_scope(buf: &[u8]) -> Result<InstrumentationScope, Error> {
    InstrumentationScope::decode_length_delimited(buf).map_err(OltpMmapError::ProtobufDecodeError)
}

/// Actually reads the span references from chunks.
fn read_span_ref(buf: &[u8]) -> Result<OtlpSpan, Error> {
    // We hide the prost message here...
    let span_ref = SpanRef::decode_length_delimited(buf)?;
    Ok(OtlpSpan {
        // span: span_ref.span,
        resource: span_ref.resource_ref,
        scope: span_ref.scope_ref,
        span: span_ref.span,
    })
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
        write!(
            f,
            "resource: {}, scope: {}, span: {}",
            self.resource, self.scope, self.span.name
        )
    }
}
