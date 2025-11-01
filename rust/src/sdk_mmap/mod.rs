//! SDK MMap file reading components.

pub mod data;
pub mod dictionary;
pub mod reader;
pub mod ringbuffer;

use std::{collections::HashMap, path::Path, time::Duration};

use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
pub use reader::MmapReader;

use crate::{
    oltp_mmap::Error,
    sdk_mmap::data::{KeyValueRef, SpanEvent},
};

/// Implementation of an OpenTelemetry SDK that pulls in events from an MMap file.
pub struct CollectorSdk {
    reader: MmapReader,
}
impl CollectorSdk {
    pub fn new(path: &Path) -> Result<CollectorSdk, Error> {
        Ok(CollectorSdk {
            reader: MmapReader::new(path)?,
        })
    }

    pub async fn dev_null_events(&self) -> Result<(), Error> {
        loop {
            let _ = self.reader.events.next().await?;
            ()
        }
    }

    pub async fn dev_null_metrics(&self) -> Result<(), Error> {
        loop {
            let _ = self.reader.metrics.next().await?;
            ()
        }
    }

    /// Open an OTLP connection and fires traces at it.
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
        let mut spans = ActiveSpans::new();
        loop {
            // TODO - check_sanity()
            // TODO - Config
            let span_batch = spans
                .try_buffer_spans(&self, 100, Duration::from_secs(60))
                .await?;
            let next_batch = self.try_create_span_batch(span_batch).await?;
            if !next_batch.resource_spans.is_empty() {
                println!("Sending batch #{batch_idx}");
                endpoint.export(next_batch).await?;
                batch_idx += 1;
            }
        }
    }

    /// Converts a batch of tracked spans into OTLP batch of spans using dictionary lookup.
    async fn try_create_span_batch(
        &self,
        batch: Vec<TrackedSpan>,
    ) -> Result<opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest, Error>
    {
        // TODO - handle empty batch.
        let mut scope_map: HashMap<i64, Vec<opentelemetry_proto::tonic::trace::v1::Span>> =
            HashMap::new();
        for span in batch {
            scope_map
                .entry(span.scope_ref)
                .or_insert(Vec::new())
                .push(span.current);
        }

        let mut resource_map: HashMap<
            i64,
            Vec<(
                i64,
                opentelemetry_proto::tonic::common::v1::InstrumentationScope,
            )>,
        > = HashMap::new();
        for scope_ref in scope_map.keys() {
            let scope = self.try_lookup_scope(*scope_ref).await?;
            resource_map
                .entry(scope.resource_ref)
                .or_insert(Vec::new())
                .push((*scope_ref, scope.scope));
        }

        let mut result =
            opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest {
                resource_spans: Default::default(),
            };
        for (resource_ref, scopes) in resource_map.into_iter() {
            let resource = self.try_lookup_resource(resource_ref).await?;
            let mut resource_spans = opentelemetry_proto::tonic::trace::v1::ResourceSpans {
                resource: Some(resource),
                scope_spans: Default::default(),
                // TODO - pull this.
                schema_url: "".to_owned(),
            };
            for (sid, scope) in scopes.into_iter() {
                let mut scope_spans = opentelemetry_proto::tonic::trace::v1::ScopeSpans {
                    scope: Some(scope),
                    spans: Vec::new(),
                    // TODO - pull this
                    schema_url: "".to_owned(),
                };
                if let Some(spans) = scope_map.remove(&sid) {
                    scope_spans.spans.extend(spans);
                }
                resource_spans.scope_spans.push(scope_spans);
            }
            result.resource_spans.push(resource_spans);
        }
        Ok(result)
    }

    async fn try_lookup_resource(
        &self,
        resource_ref: i64,
    ) -> Result<opentelemetry_proto::tonic::resource::v1::Resource, Error> {
        let resource: data::Resource = self.reader.dictionary.try_read(resource_ref).await?;
        let mut attributes = Vec::new();
        for kv in resource.attributes {
            attributes.push(self.try_convert_attribute(kv).await?);
        }
        Ok(opentelemetry_proto::tonic::resource::v1::Resource {
            attributes,
            dropped_attributes_count: resource.dropped_attributes_count,
            // TODO - support entities.
            entity_refs: Vec::new(),
        })
    }

    // Looks up the scope from the dictionary (note: expensive).
    async fn try_lookup_scope(&self, scope_ref: i64) -> Result<PartialScope, Error> {
        let scope: data::InstrumentationScope = self.reader.dictionary.try_read(scope_ref).await?;
        let mut attributes = Vec::new();
        for kv in scope.attributes {
            attributes.push(self.try_convert_attribute(kv).await?);
        }
        let name: String = self.reader.dictionary.try_read_string(scope.name_ref).await?;
        let version: String = self.reader.dictionary.try_read_string(scope.version_ref).await?;
        Ok(PartialScope {
            scope: opentelemetry_proto::tonic::common::v1::InstrumentationScope {
                name,
                version,
                attributes,
                dropped_attributes_count: scope.dropped_attributes_count,
            },
            resource_ref: scope.resource_ref,
        })
    }

    /// Converts a key-value pair reference by looking up key strings in the dictionary.
    async fn try_convert_attribute(
        &self,
        kv: KeyValueRef,
    ) -> Result<opentelemetry_proto::tonic::common::v1::KeyValue, Error> {
        let key= match self.reader.dictionary.try_read_string(kv.key_ref).await {
            Ok(value) => value,
            // TODO - remove this, once we fix dictionary lookup.
            Err(_) => "<not found>".to_owned(),
        };
        let value = match kv.value {
            Some(data::AnyValue {
                value: Some(data::any_value::Value::StringValue(s)),
            }) => Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                value: Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s),
                ),
            }),
            Some(data::AnyValue {
                value: Some(data::any_value::Value::BoolValue(b)),
            }) => Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b)),
            }),
            Some(data::AnyValue {
                value: Some(data::any_value::Value::IntValue(v)),
            }) => Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(v)),
            }),
            Some(data::AnyValue {
                value: Some(data::any_value::Value::DoubleValue(v)),
            }) => Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                value: Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(v),
                ),
            }),
            // TODO - handle more
            _ => None,
        };
        Ok(opentelemetry_proto::tonic::common::v1::KeyValue { key, value })
    }
}

struct PartialScope {
    pub scope: opentelemetry_proto::tonic::common::v1::InstrumentationScope,
    pub resource_ref: i64,
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
struct FullSpanId {
    trace_id: [u8; 16],
    span_id: [u8; 8],
}
impl FullSpanId {
    fn try_from_event(e: &SpanEvent) -> Result<FullSpanId, Error> {
        Ok(FullSpanId {
            trace_id: e.trace_id.as_slice().try_into()?,
            span_id: e.span_id.as_slice().try_into()?,
        })
    }
}

fn bytes_to_hex_string(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{:02x}", byte)) // Format each byte as a two-digit lowercase hex
        .collect() // Collect the formatted strings into a single String
}

impl std::fmt::Display for FullSpanId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "span {} @ {}",
            bytes_to_hex_string(&self.trace_id),
            bytes_to_hex_string(&self.span_id)
        )
    }
}

// TODO - Sort out what this will need to do.
pub struct TrackedSpan {
    // Index into scope to use.
    pub scope_ref: i64,
    pub current: opentelemetry_proto::tonic::trace::v1::Span,
}

struct ActiveSpans {
    spans: HashMap<FullSpanId, TrackedSpan>, // TODO a cache for lookups we need to send spans,
                                             // e.g. scope, resource, attribute key names.
}

impl ActiveSpans {
    fn new() -> ActiveSpans {
        ActiveSpans {
            spans: HashMap::new(),
        }
    }

    /// Reads events, tracking spans and attempts to construct a buffer.
    ///
    /// If timeout is met before buffer is filled, the buffer is returned.
    async fn try_buffer_spans(
        &mut self,
        sdk: &CollectorSdk,
        len: usize,
        timeout: tokio::time::Duration,
    ) -> Result<Vec<TrackedSpan>, Error> {
        // TODO - check sanity on the file before continuing.
        // Here we create a batch of spans.
        let mut buf = Vec::new();
        let send_by_time =
            // TODO - configurable batch timeouts.
            tokio::time::sleep_until(tokio::time::Instant::now() + timeout);
        tokio::pin!(send_by_time);
        loop {
            tokio::select! {
                event = sdk.reader.spans.next() => {
                    if let Some(span) = self.try_handle_span_event(event?, sdk).await? {
                        println!("Span {:?} completed, adding to buffer", span.current);
                        buf.push(span);
                        // TODO - configure the size of this.
                        if buf.len() >= len {
                            return Ok(buf)
                        }
                    }
                },
                () = &mut send_by_time => {
                    return Ok(buf)
                }
            }
        }
    }

    /// Handles a span event.
    ///
    /// Returns a span, if this event has completed it.
    async fn try_handle_span_event(
        &mut self,
        e: SpanEvent,
        attr_lookup: &CollectorSdk,
    ) -> Result<Option<TrackedSpan>, Error> {
        let hash = FullSpanId::try_from_event(&e)?;
        match e.event {
            Some(data::span_event::Event::Start(start)) => {
                // TODO - optimise attribute load
                let mut attributes = Vec::new();
                for kvr in start.attributes {
                    attributes.push(attr_lookup.try_convert_attribute(kvr).await?);
                }
                let span_state = opentelemetry_proto::tonic::trace::v1::Span {
                    trace_id: e.trace_id,
                    span_id: e.span_id,
                    // TODO - make sure we record trace state.
                    trace_state: "".into(),
                    parent_span_id: start.parent_span_id,
                    flags: start.flags,
                    name: start.name,
                    kind: start.kind,
                    start_time_unix_nano: start.start_time_unix_nano,
                    attributes,
                    // Things we don't have yet.
                    end_time_unix_nano: 0,
                    dropped_attributes_count: 0,
                    events: Vec::new(),
                    dropped_events_count: 0,
                    links: Vec::new(),
                    dropped_links_count: 0,
                    status: None,
                };
                self.spans.insert(
                    hash,
                    TrackedSpan {
                        current: span_state,
                        scope_ref: e.scope_ref,
                    },
                );
            }
            Some(data::span_event::Event::Link(_)) => todo!(),
            Some(data::span_event::Event::Name(ne)) => {
                if let Some(entry) = self.spans.get_mut(&hash) {
                    entry.current.name = ne.name;
                }
            }
            Some(data::span_event::Event::Attributes(ae)) => {
                // TODO - optimise attribute load
                if let Some(entry) = self.spans.get_mut(&hash) {
                    for kvr in ae.attributes {
                        entry
                            .current
                            .attributes
                            .push(attr_lookup.try_convert_attribute(kvr).await?);
                    }
                }
            }
            Some(data::span_event::Event::End(se)) => {
                if let Some(mut entry) = self.spans.remove(&hash) {
                    entry.current.end_time_unix_nano = se.end_time_unix_nano;
                    if let Some(status) = se.status {
                        entry.current.status = Some(opentelemetry_proto::tonic::trace::v1::Status {
                            message: status.message,
                            code: status.code,
                        })
                    }
                    return Ok(Some(entry));
                }
            }
            // Log the issue vs. crash.
            None => todo!("logic error!"),
        }
        // TODO - garbage collection if dangling spans is too high?
        Ok(None)
    }
}
