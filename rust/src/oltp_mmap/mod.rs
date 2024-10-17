use itertools::Itertools;
use opentelemetry_proto::tonic::{
    collector::trace::{
        self,
        v1::{trace_service_client::TraceServiceClient, ExportTraceServiceRequest},
    },
    common::v1::InstrumentationScope,
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans},
};
use span_ref::SpanRef;
use std::{path::Path, time::Duration};

pub mod dictionary;
pub mod error;
pub mod ringbuffer;
pub mod span_ref;

/// Errors used within OTLP-mmap.
pub type Error = error::OltpMmapError;
type RingBufferReader<T> = ringbuffer::RingBufferReader<T>;
type DictionaryReader<T> = dictionary::DictionaryReader<T>;

/// Asynchronous exeuction of OTLP mmap input channels.
pub struct OtlpMmapReader {
    resources: DictionaryReader<Resource>,
    scopes: DictionaryReader<InstrumentationScope>,
    spans: RingBufferReader<SpanRef>,
}
impl OtlpMmapReader {
    /// Construct a new reader of OTLP mmap protocol.
    pub fn new(p: &Path) -> Result<OtlpMmapReader, Error> {
        // TODO - configurable cache capacity.
        Ok(OtlpMmapReader {
            resources: DictionaryReader::new(&p.join("resource.otlp"), 10)?,
            scopes: DictionaryReader::new(&p.join("scope.otlp"), 100)?,
            spans: RingBufferReader::new(&p.join("spans.otlp"))?,
        })
    }

    // Opens an OTLP connection and fires traces read from OTLP mmap into it.
    pub async fn send_traces_to(&self, trace_endpoint: &str) -> Result<(), Error> {
        let client = TraceServiceClient::connect(trace_endpoint.to_owned()).await?;
        self.send_traces_loop(client).await
    }

    /// This will loop and attempt to send traces at an OTLP endpoint.
    /// Continuing infinitely.
    async fn send_traces_loop(
        &self,
        mut endpoint: TraceServiceClient<tonic::transport::Channel>,
    ) -> Result<(), Error> {
        let mut batch_idx = 1;
        loop {
            self.check_sanity().await?;
            let next_batch = self.create_otlp_trace_write_request().await?;
            if !next_batch.resource_spans.is_empty() {
                println!("Sending batch #{batch_idx}");
                endpoint.export(next_batch).await?;
                batch_idx += 1;
            }
        }
    }

    /// Returns OK(()) if OTLP MMAP versions match, error otherwise.
    async fn check_sanity(&self) -> Result<(), Error> {
        // TODO - check sanity should check if our local version read is still the same,
        // vs.just comparing version against each other.
        let r_version = self.resources.version().await;
        let s_version = self.scopes.version().await;
        if r_version != s_version {
            return Err(error::OltpMmapError::VersionMismatch(r_version, s_version))
        }
        let version = self.spans.version().await;
        if r_version != version {
            return Err(error::OltpMmapError::VersionMismatch(r_version, version));
        }
        Ok(())        
    }

    /// Constructs an OTLP (not mmap) TraceServiceRequest.
    /// This will first atempt to buffer 100 spans (or elapsed time) before returning.
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
                resource: Some(resource.as_ref().clone()),
                scope_spans: Default::default(),
                schema_url: "".to_owned(),
            };
            for (sid, spans) in &spans.chunk_by(|s| s.scope_ref) {
                let scope = self.scopes.get(sid).await?;
                resource_spans.scope_spans.push(ScopeSpans {
                    scope: Some(scope.as_ref().clone()),
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
            tokio::time::sleep_until(tokio::time::Instant::now() + Duration::from_secs(60));
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
