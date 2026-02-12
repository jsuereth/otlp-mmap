//! API and tests for collecting MMAP SDK files into OTLP messages.
//!
//! This package defines necessary components and methods to read an OTLP-mmap file and
//! convert it into vanilla OTLP messages that can fired at an OTLP endpoint.
//!
//! This should mirror the implementation behavior of an OpenTelemetry SDK and provide
//! compliance to its specification.

mod error;
pub mod log;
pub mod metric;
/// Tracing event handler.
pub mod trace;

use std::{collections::HashMap, path::Path, time::Duration};

pub use error::Error;
use opentelemetry_proto::tonic::collector::{
    logs::v1::logs_service_client::LogsServiceClient,
    metrics::v1::metrics_service_client::MetricsServiceClient,
    trace::v1::trace_service_client::TraceServiceClient,
};
use otlp_mmap_core::{OtlpDictionary, OtlpMmapReader, PartialScope, RingBufferReader};
use otlp_mmap_protocol::KeyValueRef;

use crate::{
    log::EventCollector,
    metric::{CollectedMetric, MetricStorage},
    trace::{ActiveSpans, TrackedSpan},
};

/// Implementation of an OpenTelemetry SDK that pulls in events from an MMap file.
pub struct CollectorSdk {
    reader: OtlpMmapReader,
}

/// Creates a new collector sdk.
pub fn new_collector_sdk(path: &Path) -> Result<CollectorSdk, Error> {
    Ok(CollectorSdk {
        reader: OtlpMmapReader::new(path)?,
    })
}

impl CollectorSdk {
    /// Records metrics from the ringbuffer and repor them at an interval.
    pub async fn record_metrics(&self, metric_endpoint: &str) -> Result<(), Error> {
        println!("Starting metrics pipeline");
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
                    m = self.reader.metrics().try_read_next() => {
                        metric_storage.handle_measurement(self.reader.dictionary(), m?)?
                    },
                    _ = &mut send_by_time => {
                        let metrics = metric_storage.collect(&metric::CollectionContext::new(self.reader.start_time(), 0));
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
            let scope = self.reader.dictionary().try_lookup_scope(*scope_ref)?;
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
            let resource = self.reader.dictionary().try_lookup_resource(resource_ref)?;
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
        println!("Starting logs pipeline");
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
                    self.reader.events(),
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
        println!("Starting trace pipeline");
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
                    self.reader.spans(),
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
            let scope = self.reader.dictionary().try_lookup_scope(*scope_ref)?;
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
            let resource = self.reader.dictionary().try_lookup_resource(resource_ref)?;
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

// TODO - what traits to we want to expose against otlp-mmap-core to improve testing of this crate?

// TODO - do we still need this?
/// Attribute lookup trait used so we can write tests without creating an mmap file.
pub trait AttributeLookup {
    /// Converts an OTLP-MMAP KeyValueRef, performing dictionary lookups as needed.
    fn try_convert_attribute(
        &self,
        kv: KeyValueRef,
    ) -> Result<opentelemetry_proto::tonic::common::v1::KeyValue, Error>;
}

/// Lookup trait for non-attribute SDK things.
pub trait SdkLookup: AttributeLookup {
    /// Looks up a scope from the dictionary by reference.
    fn try_lookup_scope(&self, instrumentation_scope_ref: i64) -> Result<PartialScope, Error>;
    /// Looks up a resource from the dictionary by reference.
    fn try_lookup_resource(
        &self,
        resource_ref: i64,
    ) -> Result<opentelemetry_proto::tonic::resource::v1::Resource, Error>;
    /// Looks up a string from the dictionary by reference.
    fn try_read_string(&self, string_ref: i64) -> Result<String, Error>;
    /// Looks up an OTLP-MMAP metric definition.
    fn try_lookup_metric(&self, metric_ref: i64) -> Result<otlp_mmap_protocol::MetricRef, Error>;
    /// Converts an OTLP-MMAP AnyValue, performing dictionary lookups as needed.
    fn try_convert_anyvalue(
        &self,
        value: otlp_mmap_protocol::AnyValue,
    ) -> Result<Option<opentelemetry_proto::tonic::common::v1::AnyValue>, Error>;
}
/// Abstract trait to interact with ring buffers.
pub trait AsyncEventQueue<T>
where
    T: prost::Message + std::default::Default + 'static + Sync,
{
    /// Asynchronously read next value.  THis will not return until a value is available.
    async fn try_read_next(&self) -> Result<T, Error>;
}

impl AttributeLookup for OtlpDictionary {
    fn try_convert_attribute(
        &self,
        kv: KeyValueRef,
    ) -> Result<opentelemetry_proto::tonic::common::v1::KeyValue, Error> {
        Ok(self.try_convert_kv(kv)?)
    }
}

/// SdkLookup traits for otlp-mmap-core.
impl SdkLookup for OtlpDictionary {
    fn try_lookup_scope(&self, instrumentation_scope_ref: i64) -> Result<PartialScope, Error> {
        Ok(self.try_lookup_scope(instrumentation_scope_ref)?)
    }

    fn try_lookup_resource(
        &self,
        resource_ref: i64,
    ) -> Result<opentelemetry_proto::tonic::resource::v1::Resource, Error> {
        Ok(self.try_lookup_resource(resource_ref)?)
    }

    fn try_read_string(&self, string_ref: i64) -> Result<String, Error> {
        Ok(self.try_lookup_string(string_ref)?)
    }

    fn try_lookup_metric(&self, metric_ref: i64) -> Result<otlp_mmap_protocol::MetricRef, Error> {
        Ok(self.try_lookup_metric_stream(metric_ref)?)
    }

    fn try_convert_anyvalue(
        &self,
        value: otlp_mmap_protocol::AnyValue,
    ) -> Result<Option<opentelemetry_proto::tonic::common::v1::AnyValue>, Error> {
        Ok(self.try_convert_anyvalue(value)?)
    }
}

/// Implementaton of AsyncEventQueue over the "raw" otlp-mmap-core readers.
impl<T: prost::Message + std::default::Default + 'static + Sync> AsyncEventQueue<T>
    for RingBufferReader<T>
{
    /// Exponential back-off spin-lock reading.
    async fn try_read_next(&self) -> Result<T, Error> {
        for _ in 0..10 {
            if let Some(result) = self.try_read()? {
                // println!("Read {} event on fast path", std::any::type_name::<T>());
                return Ok(result);
            } else {
                tokio::task::yield_now().await;
            }
        }
        // Sleep spin, exponentially slower.
        let mut d = Duration::from_millis(1);
        loop {
            if let Some(result) = self.try_read()? {
                // println!("Read {} event on slow path", std::any::type_name::<T>());
                return Ok(result);
            } else {
                tokio::time::sleep(d).await;
            }
            // TODO - Cap max wait time configuration.
            if d.as_secs() < 1 {
                d *= 2;
            }
        }
    }
}
