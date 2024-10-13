use std::{ops::Deref, path::Path};

use dictionary::DictionaryInputChannel;
use opentelemetry_proto::tonic::{common::v1::InstrumentationScope, resource::{self, v1::Resource}, trace::v1::Span};
use prost::Message;
use ringbuffer::RingbufferInputChannel;
use span_ref::SpanRef;


pub mod ringbuffer;
pub mod dictionary;
pub mod span_ref;

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

    pub fn resource(&self, idx: i64) -> Option<Resource> {
        self.resources.entry(idx).and_then(|buf| {
            match Resource::decode_length_delimited(buf.deref()) {
                Ok(resource) => Some(resource),
                // TODO - Save errors.
                Err(e) => {
                    println!("Failed to read resource @ {idx}, {e:?}");
                    None
                },
            }
        })
    }

    pub fn scope(&self, idx: i64) -> Option<InstrumentationScope> {
        self.scopes.entry(idx).and_then(|buf| {
            match InstrumentationScope::decode_length_delimited(buf.deref()) {
                Ok(resource) => Some(resource),
                // TODO - Save errors.
                Err(e) => {
                    println!("Failed to read scope @ {idx}, {e:?}");
                    None
                },
            }
        })
    }

    // TODO - error handling.
    pub fn next_span(&mut self) -> Option<OtlpSpan> {
         // TODO - every now and then check sanity before continuing...?
        let buf = self.spans.next();
        match SpanRef::decode_length_delimited(buf.deref()) {
            Ok(span_ref) => {
                // TODO - load resource.  Use safe mechanism to do it.
                Some(OtlpSpan {
                    // span: span_ref.span,
                    resource: span_ref.resource_ref,
                    scope: span_ref.scope_ref,
                    span: span_ref.span,
                })
            },
            // TODO - save errors
            Err(e) => {
                println!("Failed to read span, {e:?}");
                None
            },
        }
    }
}

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