//! SDK MMap file collector.
//!
//! This package define necessary components and methods to read an OTLP-mmap file and
//! convert it into vanilla OTLP messages that can fired at an OTLP endpoint.
//!
//! This should mirror the implementation behavior of an OpenTelemetry SDK and provide
//! compliance to its specification.

pub mod data;
pub mod dictionary;
mod error;
mod log;
mod metric;
pub mod reader;
pub mod ringbuffer;
mod trace;

pub use crate::sdk_mmap::error::Error;
use crate::sdk_mmap::{
    data::KeyValueRef,
    dictionary::AsyncDictionary,
    log::EventCollector,
    metric::{CollectedMetric, MetricStorage},
    reader::AsyncMmapReader,
    ringbuffer::AsyncEventQueue,
};
use opentelemetry_proto::tonic::collector::{
    logs::v1::logs_service_client::LogsServiceClient,
    metrics::v1::metrics_service_client::MetricsServiceClient,
    trace::v1::trace_service_client::TraceServiceClient,
};
pub use reader::MmapReader;
use std::{collections::HashMap, path::Path, time::Duration};
use trace::{ActiveSpans, TrackedSpan};

/// Implementation of an OpenTelemetry SDK that pulls in events from an MMap file.
pub struct CollectorSdk<T: AsyncMmapReader> {
    reader: T,
}

/// Creates a new collector sdk.
pub fn new_collector_sdk(path: &Path) -> Result<CollectorSdk<MmapReader>, Error> {
    Ok(CollectorSdk {
        reader: MmapReader::new(path)?,
    })
}

impl<T: AsyncMmapReader> CollectorSdk<T> {
    /// Records metrics from the ringbuffer and repor them at an interval.
    pub async fn record_metrics(&self, metric_endpoint: &str) -> Result<(), Error> {
        // TODO - we need to set up a timer to export metrics periodically.
        let mut client = MetricsServiceClient::connect(metric_endpoint.to_owned()).await?;
        let mut metric_storage = MetricStorage::new();
        // Report metrics every minute.
        let report_interval = tokio::time::Duration::from_secs(60);
        loop {
            // If the file is out of date, bail on this reading.
            if self.reader.has_file_changed() {
                return Err(Error::OtlpMmapOutofData);
            }
            // TODO - Configuration.
            let send_by_time =
                tokio::time::sleep_until(tokio::time::Instant::now() + report_interval);
            tokio::pin!(send_by_time);
            loop {
                tokio::select! {
                    m = self.reader.measurement_queue().try_read_next() => {
                        metric_storage.handle_measurement(self.reader.dictionary(), m?).await?
                    },
                    _ = &mut send_by_time => {
                        let metrics = metric_storage.collect(&metric::CollectionContext::new(self.reader.start_time(), 0)).await;
                        // TODO - send the metrics.
                        let batch = self.try_create_metric_batch(metrics).await?;
                        // TODO - check response for retry, etc.
                        let _ = client.export(batch).await?;
                        // Go back to outer loop and reset report time.
                        break;
                    }
                }
            }
        }
    }

    /// Converts a batch of tracked spans into OTLP batch of spans using dictionary lookup.
    async fn try_create_metric_batch(
        &self,
        batch: Vec<CollectedMetric>,
    ) -> Result<
        opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest,
        Error,
    > {
        let mut scope_map: HashMap<i64, Vec<opentelemetry_proto::tonic::metrics::v1::Metric>> =
            HashMap::new();
        for metric in batch {
            scope_map
                .entry(metric.scope_ref)
                .or_default()
                .push(metric.metric);
        }
        let mut resource_map: HashMap<
            i64,
            Vec<(
                i64,
                opentelemetry_proto::tonic::common::v1::InstrumentationScope,
            )>,
        > = HashMap::new();
        for scope_ref in scope_map.keys() {
            let scope = try_lookup_scope(self.reader.dictionary(), *scope_ref).await?;
            resource_map
                .entry(scope.resource_ref)
                .or_default()
                .push((*scope_ref, scope.scope));
        }

        let mut result =
            opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest {
                resource_metrics: Default::default(),
            };
        for (resource_ref, scopes) in resource_map.into_iter() {
            let resource = try_lookup_resource(self.reader.dictionary(), resource_ref).await?;
            let mut resource_metrics = opentelemetry_proto::tonic::metrics::v1::ResourceMetrics {
                resource: Some(resource),
                scope_metrics: Default::default(),
                // TODO - pull this
                schema_url: "".to_owned(),
            };
            for (sid, scope) in scopes.into_iter() {
                let mut scope_metrics = opentelemetry_proto::tonic::metrics::v1::ScopeMetrics {
                    scope: Some(scope),
                    metrics: Vec::new(),
                    // TODO - pull this
                    schema_url: "".to_owned(),
                };
                if let Some(metrics) = scope_map.remove(&sid) {
                    scope_metrics.metrics.extend(metrics);
                    resource_metrics.scope_metrics.push(scope_metrics);
                }
            }
            result.resource_metrics.push(resource_metrics);
        }
        Ok(result)
    }

    pub async fn send_logs_to(&self, log_endpoint: &str) -> Result<(), Error> {
        let client = LogsServiceClient::connect(log_endpoint.to_owned()).await?;
        // TODO - if this fails, reopen SDK file and start again?
        self.send_events_loop(client).await
    }

    async fn send_events_loop(
        &self,
        mut endpoint: LogsServiceClient<tonic::transport::Channel>,
    ) -> Result<(), Error> {
        // let mut batch_idx = 1;
        let mut collector = EventCollector::new();
        loop {
            // TODO - config.
            // If the file is out of date, bail on this reading.
            if self.reader.has_file_changed() {
                return Err(Error::OtlpMmapOutofData);
            }
            // println!("Batching logs");
            if let Some(log_batch) = collector
                .try_create_next_batch(
                    self.reader.event_queue(),
                    self.reader.dictionary(),
                    1000,
                    Duration::from_secs(60),
                )
                .await?
            {
                // println!("Sending log batch #{batch_idx}");
                endpoint.export(log_batch).await?;
                // batch_idx += 1;
            }
        }
    }

    /// Open an OTLP connection and fires traces at it.
    pub async fn send_traces_to(&self, trace_endpoint: &str) -> Result<(), Error> {
        let client = TraceServiceClient::connect(trace_endpoint.to_owned()).await?;
        // TODO - if this fails, reopen SDK file and start again?
        self.send_traces_loop(client).await
    }

    /// This will loop and attempt to send traces at an OTLP endpoint.
    /// Continuing infinitely.
    async fn send_traces_loop(
        &self,
        mut endpoint: TraceServiceClient<tonic::transport::Channel>,
    ) -> Result<(), Error> {
        // let mut batch_idx = 1;
        let mut spans = ActiveSpans::new();
        loop {
            // If the file is out of date, bail on this reading.
            if self.reader.has_file_changed() {
                return Err(Error::OtlpMmapOutofData);
            }
            // TODO - Config
            // println!("Batching spans");
            let span_batch = spans
                .try_buffer_spans(
                    self.reader.spans_queue(),
                    self.reader.dictionary(),
                    1000,
                    Duration::from_secs(60),
                )
                .await?;
            let next_batch = self.try_create_span_batch(span_batch).await?;
            if !next_batch.resource_spans.is_empty() {
                // println!("Sending span batch #{batch_idx}");
                endpoint.export(next_batch).await?;
                // batch_idx += 1;
            } else {
                // println!("No new batch of spans, in-flight spans: {}", spans.num_active());
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
                .or_default()
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
            let scope = try_lookup_scope(self.reader.dictionary(), *scope_ref).await?;
            resource_map
                .entry(scope.resource_ref)
                .or_default()
                .push((*scope_ref, scope.scope));
        }

        let mut result =
            opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest {
                resource_spans: Default::default(),
            };
        for (resource_ref, scopes) in resource_map.into_iter() {
            let resource = try_lookup_resource(self.reader.dictionary(), resource_ref).await?;
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
}

/// A scope with reference to its resource in the dictionary.
struct PartialScope {
    /// The instrumentation scope.
    pub scope: opentelemetry_proto::tonic::common::v1::InstrumentationScope,
    /// Reference to a resource in the dictionary.
    pub resource_ref: i64,
}

/// Attribute lookup trait used so we can write tests without creating an mmap file.
pub trait AttributeLookup {
    // TODO - do we still need this?
    async fn try_convert_attribute(
        &self,
        kv: KeyValueRef,
    ) -> Result<opentelemetry_proto::tonic::common::v1::KeyValue, Error>;
}

/// Converts a key-value pair reference by looking up key strings in the dictionary.
async fn try_convert_attribute<T: AsyncDictionary>(
    lookup: &T,
    kv: KeyValueRef,
) -> Result<opentelemetry_proto::tonic::common::v1::KeyValue, Error> {
    let key = match lookup.try_read_string(kv.key_ref).await {
        Ok(value) => value,
        // TODO - remove this, once we fix dictionary lookup.
        Err(_) => "<not found>".to_owned(),
    };
    let value = if let Some(v) = kv.value {
        Box::pin(try_convert_anyvalue(lookup, v)).await?
    } else {
        None
    };
    Ok(opentelemetry_proto::tonic::common::v1::KeyValue { key, value })
}

/// Convert an anyvalue that may have references to dictionary entries into a full anyvalue.
async fn try_convert_anyvalue<T: AsyncDictionary>(
    lookup: &T,
    value: data::AnyValue,
) -> Result<Option<opentelemetry_proto::tonic::common::v1::AnyValue>, Error> {
    let result = match value.value {
        Some(data::any_value::Value::StringValue(v)) => {
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(v))
        }
        Some(data::any_value::Value::BoolValue(v)) => {
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(v))
        }
        Some(data::any_value::Value::IntValue(v)) => {
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(v))
        }
        Some(data::any_value::Value::DoubleValue(v)) => {
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(v))
        }
        Some(data::any_value::Value::BytesValue(v)) => {
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BytesValue(v))
        }
        Some(data::any_value::Value::ArrayValue(v)) => {
            let mut values = Vec::new();

            for av in v.values {
                if let Some(rav) = Box::pin(try_convert_anyvalue(lookup, av)).await? {
                    values.push(rav);
                }
            }
            Some(
                opentelemetry_proto::tonic::common::v1::any_value::Value::ArrayValue(
                    opentelemetry_proto::tonic::common::v1::ArrayValue { values },
                ),
            )
        }
        Some(data::any_value::Value::KvlistValue(kvs)) => {
            // TODO - implement.
            let mut values = Vec::new();
            for kv in kvs.values {
                values.push(lookup.try_convert_attribute(kv).await?);
            }
            Some(
                opentelemetry_proto::tonic::common::v1::any_value::Value::KvlistValue(
                    opentelemetry_proto::tonic::common::v1::KeyValueList { values },
                ),
            )
        }
        Some(data::any_value::Value::ValueRef(idx)) => {
            // TODO - try to improve performance here.
            let v: data::AnyValue = Box::pin(lookup.try_read(idx)).await?;
            Box::pin(try_convert_anyvalue(lookup, v))
                .await?
                .and_then(|v| v.value)
        }
        None => None,
    };
    Ok(result.map(|value| opentelemetry_proto::tonic::common::v1::AnyValue { value: Some(value) }))
}

/// Perform a resource lookup, including attribute lookups / conversion, for a resource.
pub async fn try_lookup_resource<T: AsyncDictionary>(
    lookup: &T,
    resource_ref: i64,
) -> Result<opentelemetry_proto::tonic::resource::v1::Resource, Error> {
    let resource: data::Resource = lookup.try_read(resource_ref).await?;
    let mut attributes = Vec::new();
    for kv in resource.attributes {
        attributes.push(try_convert_attribute(lookup, kv).await?);
    }
    Ok(opentelemetry_proto::tonic::resource::v1::Resource {
        attributes,
        dropped_attributes_count: resource.dropped_attributes_count,
        // TODO - support entities.
        entity_refs: Vec::new(),
    })
}

// Looks up the scope from the dictionary (note: expensive).
async fn try_lookup_scope<T: AsyncDictionary>(
    lookup: &T,
    scope_ref: i64,
) -> Result<PartialScope, Error> {
    let scope: data::InstrumentationScope = lookup.try_read(scope_ref).await?;
    let mut attributes = Vec::new();
    for kv in scope.attributes {
        attributes.push(try_convert_attribute(lookup, kv).await?);
    }
    let name: String = lookup.try_read_string(scope.name_ref).await?;
    let version: String = lookup.try_read_string(scope.version_ref).await?;
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

impl<T: AsyncDictionary> AttributeLookup for T {
    /// Converts a key-value pair reference by looking up key strings in the dictionary.
    async fn try_convert_attribute(
        &self,
        kv: KeyValueRef,
    ) -> Result<opentelemetry_proto::tonic::common::v1::KeyValue, Error> {
        try_convert_attribute(self, kv).await
    }
}
