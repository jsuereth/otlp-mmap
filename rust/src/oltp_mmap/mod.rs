use dictionary::{AsyncDictionaryInput, DictionaryInputChannel};
use error::OltpMmapError;
use itertools::Itertools;
use opentelemetry_proto::tonic::{
    collector::trace::{
        self,
        v1::{trace_service_client::TraceServiceClient, ExportTraceServiceRequest},
    },
    common::v1::InstrumentationScope,
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span},
};
use prost::Message;
use ringbuffer::{AsyncRingBufferInputChannel, RingbufferInputChannel};
use span_ref::SpanRef;
use std::{path::Path, time::Duration};

pub mod dictionary;
pub mod error;
pub mod ringbuffer;
pub mod span_ref;
// pub mod r#async;

/// Errors used within OTLP-mmap.
pub type Error = error::OltpMmapError;

/// Asynchronous exeuction of OTLP mmap input channels.
pub struct OtlpInputAsync {
    resources: AsyncDictionaryInput<Resource>,
    scopes: AsyncDictionaryInput<InstrumentationScope>,
    spans: AsyncRingBufferInputChannel<SpanRef>,
}
impl OtlpInputAsync {
    pub fn new(p: &Path) -> Result<OtlpInputAsync, Error> {
        Ok(OtlpInputAsync {
            resources: AsyncDictionaryInput::new(&p.join("resource.otlp"), 10)?,
            scopes: AsyncDictionaryInput::new(&p.join("scope.otlp"), 100)?,
            spans: AsyncRingBufferInputChannel::new(&p.join("spans.otlp"))?,
        })
    }

    pub async fn send_traces_to(&self, trace_endpoint: &str) -> Result<(), Error> {
        let client = TraceServiceClient::connect(trace_endpoint.to_owned()).await?;
        self.send_traces_loop(client).await
    }

    async fn send_traces_loop(
        &self,
        mut endpoint: TraceServiceClient<tonic::transport::Channel>,
    ) -> Result<(), Error> {
        loop {
            let next_batch = self.create_otlp_trace_write_request().await?;
            endpoint.export(next_batch).await?;
        }
    }

    async fn create_otlp_trace_write_request(
        &self,
    ) -> Result<trace::v1::ExportTraceServiceRequest, Error> {
        // TODO - configure buffer spans.
        let spans = self.buffer_spans(100).await?;
        let mut result = ExportTraceServiceRequest {
            resource_spans: Default::default(),
        };
        for (rid, spans) in spans.into_iter().chunk_by(|s| s.resource_ref).into_iter() {
            let resource = self.resources.get(rid).await?;
            let mut resource_spans = ResourceSpans {
                resource: Some(resource),
                scope_spans: Default::default(),
                schema_url: "".to_owned(),
            };
            for (sid, spans) in &spans.chunk_by(|s| s.scope_ref) {
                let scope = self.scopes.get(sid).await?;
                resource_spans.scope_spans.push(ScopeSpans {
                    scope: Some(scope),
                    spans: spans.into_iter().map(|s| s.span).collect(),
                    schema_url: "".to_owned(),
                });
            }
            result.resource_spans.push(resource_spans);
        }
        Ok(result)
    }

    /// Groups spans (with timeout) and sends the group for downstream publishing.
    async fn buffer_spans(&self, max_spans: usize) -> Result<Vec<SpanRef>, Error> {
        // TODO - Allow configurable timeout.
        let mut buf = Vec::new();
        let send_by_time =
            tokio::time::sleep_until(tokio::time::Instant::now() + Duration::from_secs(1));
        tokio::pin!(send_by_time);

        loop {
            tokio::select! {
                span = self.spans.next() => {
                    buf.push(span?);
                    if buf.len() >= max_spans {
                        return Ok(buf);
                    }
                },
                () = &mut send_by_time => {
                    return Ok(buf);
                },
            }
        }
    }
}

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

    /// Attempts to read the next span.  Returns Ok(None) if none are available.
    pub fn try_next_span(&mut self) -> Result<Option<OtlpSpan>, Error> {
        if let Some(buf) = self.spans.try_next() {
            Ok(Some(read_span_ref(&buf)?))
        } else {
            Ok(None)
        }
    }

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
