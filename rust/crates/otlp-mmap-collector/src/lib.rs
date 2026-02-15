//! API and tests for collecting MMAP SDK files into OTLP messages.
//!
//! This package defines necessary components and methods to read an OTLP-mmap file and
//! convert it into vanilla OTLP messages that can fired at an OTLP endpoint.
//!
//! This should mirror the implementation behavior of an OpenTelemetry SDK and provide
//! compliance to its specification.

mod config;
mod error;
pub mod log;
pub mod metric;
#[cfg(test)]
pub mod test_utils;
/// Tracing event handler.
pub mod trace;

// Re-expose errors
pub use error::Error;
// Re-expose config
pub use config::{CollectorSdkConfig, LogSdkConfig, MetricSdkConfig, TraceSdkConfig};

use opentelemetry_proto::tonic::collector::{
    logs::v1::logs_service_client::LogsServiceClient,
    metrics::v1::metrics_service_client::MetricsServiceClient,
    trace::v1::trace_service_client::TraceServiceClient,
};
use otlp_mmap_core::{OtlpDictionary, OtlpMmapReader, PartialScope, RingBufferReader};
use otlp_mmap_protocol::KeyValueRef;
use std::{collections::HashMap, path::Path, time::Duration};

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
    pub async fn record_metrics(&self, config: &MetricSdkConfig) -> Result<(), Error> {
        println!("Starting metrics pipeline");
        let mut client = MetricsServiceClient::connect(config.metric_endpoint.clone()).await?;
        let mut metric_storage = MetricStorage::new();
        // Report metrics every minute.
        let report_interval = config.report_interval;
        loop {
            // If the file is out of date, bail on this reading.
            if self.reader.has_file_changed() {
                return Err(Error::OtlpMmapOutofData);
            }
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

    pub async fn send_logs_to(&self, config: &LogSdkConfig) -> Result<(), Error> {
        println!("Starting logs pipeline");
        let client = LogsServiceClient::connect(config.log_endpoint.clone()).await?;
        // TODO - if this fails, reopen SDK file and start again?
        self.send_events_loop(client, config).await
    }

    async fn send_events_loop(
        &self,
        mut endpoint: LogsServiceClient<tonic::transport::Channel>,
        config: &LogSdkConfig,
    ) -> Result<(), Error> {
        // let mut batch_idx = 1;
        let mut collector = EventCollector::new();
        loop {
            // If the file is out of date, bail on this reading.
            if self.reader.has_file_changed() {
                return Err(Error::OtlpMmapOutofData);
            }
            // println!("Batching logs");
            if let Some(log_batch) = collector
                .try_create_next_batch(
                    self.reader.events(),
                    self.reader.dictionary(),
                    config.max_batch_length,
                    config.batch_timeout,
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
    pub async fn send_traces_to(&self, config: &TraceSdkConfig) -> Result<(), Error> {
        println!("Starting trace pipeline");
        let client = TraceServiceClient::connect(config.trace_endpoint.clone()).await?;
        // TODO - if this fails, reopen SDK file and start again?
        self.send_traces_loop(client, config).await
    }

    /// This will loop and attempt to send traces at an OTLP endpoint.
    /// Continuing infinitely.
    async fn send_traces_loop(
        &self,
        mut endpoint: TraceServiceClient<tonic::transport::Channel>,
        config: &TraceSdkConfig,
    ) -> Result<(), Error> {
        // let mut batch_idx = 1;
        let mut spans = ActiveSpans::new();
        loop {
            // If the file is out of date, bail on this reading.
            if self.reader.has_file_changed() {
                return Err(Error::OtlpMmapOutofData);
            }
            // println!("Batching spans");
            let span_batch = spans
                .try_buffer_spans(
                    self.reader.spans(),
                    self.reader.dictionary(),
                    config.max_batch_length,
                    config.batch_timeout,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::MockOtlpService;
    use core::panic;
    use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsServiceServer;
    use otlp_mmap_core::{OtlpMmapConfig, OtlpMmapWriter};
    use otlp_mmap_protocol::{Event as MmapEvent, InstrumentationScope, Resource};
    use std::sync::{Arc, Mutex};
    use tempfile::NamedTempFile;
    use tokio::sync::mpsc;
    use tonic::transport::Server;

    #[tokio::test]
    async fn test_span_grouping_logic() -> Result<(), Box<dyn std::error::Error>> {
        let file = NamedTempFile::new()?;
        let config = otlp_mmap_core::OtlpMmapConfig::default();
        let writer = OtlpMmapWriter::new(file.path(), &config)?;

        let res1_ref = writer.dictionary().try_write(&Resource {
            ..Default::default()
        })?;
        let res2_ref = writer.dictionary().try_write(&Resource {
            ..Default::default()
        })?;

        let s1_name = writer.dictionary().try_write_string("s1")?;
        let s2_name = writer.dictionary().try_write_string("s2")?;
        let v = writer.dictionary().try_write_string("1.0")?;

        let scope1_ref = writer.dictionary().try_write(&InstrumentationScope {
            name_ref: s1_name,
            version_ref: v,
            resource_ref: res1_ref,
            ..Default::default()
        })?;
        let scope2_ref = writer.dictionary().try_write(&InstrumentationScope {
            name_ref: s2_name,
            version_ref: v,
            resource_ref: res2_ref,
            ..Default::default()
        })?;

        let sdk = CollectorSdk {
            reader: OtlpMmapReader::new(file.path())?,
        };

        let batch = vec![
            TrackedSpan {
                scope_ref: scope1_ref,
                current: opentelemetry_proto::tonic::trace::v1::Span {
                    name: "span1".to_owned(),
                    ..Default::default()
                },
            },
            TrackedSpan {
                scope_ref: scope1_ref,
                current: opentelemetry_proto::tonic::trace::v1::Span {
                    name: "span2".to_owned(),
                    ..Default::default()
                },
            },
            TrackedSpan {
                scope_ref: scope2_ref,
                current: opentelemetry_proto::tonic::trace::v1::Span {
                    name: "span3".to_owned(),
                    ..Default::default()
                },
            },
        ];

        let result = sdk.try_create_span_batch(batch).await?;

        assert_eq!(result.resource_spans.len(), 2);
        // Find resource with span1
        let rs1 = result
            .resource_spans
            .iter()
            .find(|rs| {
                rs.scope_spans
                    .iter()
                    .any(|ss| ss.scope.as_ref().map(|s| s.name == "s1").unwrap_or(false))
            })
            .expect("Failed to find resoure spans with scope s1");
        assert_eq!(rs1.scope_spans.len(), 1);
        assert_eq!(rs1.scope_spans[0].spans.len(), 2);

        let rs2 = result
            .resource_spans
            .iter()
            .find(|rs| {
                rs.scope_spans
                    .iter()
                    .any(|ss| ss.scope.as_ref().expect("Resource with empty scope").name == "s2")
            })
            .expect("Failed to find resoure spans with scope s2");
        assert_eq!(rs2.scope_spans.len(), 1);
        assert_eq!(rs2.scope_spans[0].spans.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_metric_grouping_logic() -> Result<(), Box<dyn std::error::Error>> {
        let file = NamedTempFile::new()?;
        let config = otlp_mmap_core::OtlpMmapConfig::default();
        let writer = OtlpMmapWriter::new(file.path(), &config)?;

        let res1_ref = writer.dictionary().try_write(&Resource {
            ..Default::default()
        })?;
        let s1_name = writer.dictionary().try_write_string("s1")?;
        let v = writer.dictionary().try_write_string("1.0")?;

        let scope1_ref = writer.dictionary().try_write(&InstrumentationScope {
            name_ref: s1_name,
            version_ref: v,
            resource_ref: res1_ref,
            ..Default::default()
        })?;

        let sdk = CollectorSdk {
            reader: OtlpMmapReader::new(file.path())?,
        };

        let metrics = vec![CollectedMetric {
            scope_ref: scope1_ref,
            metric: opentelemetry_proto::tonic::metrics::v1::Metric {
                name: "m1".to_owned(),
                ..Default::default()
            },
        }];

        let result = sdk.try_create_metric_batch(metrics).await?;
        assert_eq!(result.resource_metrics.len(), 1);
        assert_eq!(result.resource_metrics[0].scope_metrics.len(), 1);
        assert_eq!(result.resource_metrics[0].scope_metrics[0].metrics.len(), 1);

        Ok(())
    }

    /// A reader which will delay, forcing us to ensure our timeouts are working.
    struct MockDelayedReader {
        attempts: Mutex<usize>,
        max_attempts: usize,
    }

    impl MockDelayedReader {
        fn try_read(&self) -> Result<Option<MmapEvent>, Error> {
            let mut attempts = self
                .attempts
                .lock()
                .expect("Did not expect mutax poisoning in test");
            if *attempts >= self.max_attempts {
                Ok(Some(MmapEvent::default()))
            } else {
                *attempts += 1;
                Ok(None)
            }
        }
    }

    impl AsyncEventQueue<MmapEvent> for MockDelayedReader {
        async fn try_read_next(&self) -> Result<MmapEvent, Error> {
            for _ in 0..10 {
                if let Some(result) = self.try_read()? {
                    return Ok(result);
                } else {
                    tokio::task::yield_now().await;
                }
            }
            let mut d = Duration::from_millis(1);
            loop {
                if let Some(result) = self.try_read()? {
                    return Ok(result);
                } else {
                    tokio::time::sleep(d).await;
                }
                if d.as_secs() < 1 {
                    d *= 2;
                }
            }
        }
    }

    #[tokio::test]
    async fn test_async_read_backoff() -> Result<(), Error> {
        tokio::time::pause();
        let reader = MockDelayedReader {
            attempts: Mutex::new(0),
            max_attempts: 15, // 10 yields + 5 sleeps
        };

        let read_future = reader.try_read_next();
        tokio::pin!(read_future);

        // Should not be ready yet
        tokio::select! {
            _ = &mut read_future => panic!("Should not be ready"),
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }

        // Advance time to trigger sleeps
        tokio::time::advance(Duration::from_millis(100)).await;

        let result = read_future.await?;
        assert_eq!(result, MmapEvent::default());

        Ok(())
    }

    #[tokio::test]
    async fn test_file_change_detection() -> Result<(), Box<dyn std::error::Error>> {
        let file = NamedTempFile::new()?;
        let config = otlp_mmap_core::OtlpMmapConfig::default();

        // Initial write
        {
            let _writer = OtlpMmapWriter::new(file.path(), &config)?;
        }

        let sdk = CollectorSdk {
            reader: OtlpMmapReader::new(file.path())?,
        };

        assert!(!sdk.reader.has_file_changed());

        // Simulate file change by re-initializing with a new writer (which updates start_time)
        tokio::time::sleep(Duration::from_millis(10)).await;
        {
            let _writer2 = OtlpMmapWriter::new(file.path(), &config)?;
        }

        assert!(sdk.reader.has_file_changed());

        Ok(())
    }

    #[tokio::test]
    async fn test_record_metrics_connect_failure() -> Result<(), Box<dyn std::error::Error>> {
        let file = NamedTempFile::new()?;
        // This will set up the mmap file to avoid errors.
        let writer_config = OtlpMmapConfig::default();
        let _ = OtlpMmapWriter::new(file.path(), &writer_config)?;

        let sdk = CollectorSdk {
            reader: OtlpMmapReader::new(file.path())?,
        };

        // Invalid URL should cause connect failure
        let config = MetricSdkConfig {
            metric_endpoint: "http://domain.invalid:4317".to_owned(),
            ..Default::default()
        };
        let result = sdk.record_metrics(&config).await;
        assert!(result.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_end_to_end_logs_batching() -> Result<(), Box<dyn std::error::Error>> {
        let file = NamedTempFile::new()?;
        let config = otlp_mmap_core::OtlpMmapConfig::default();
        let writer = OtlpMmapWriter::new(file.path(), &config)?;

        // Setup dictionary
        let res_ref = writer.dictionary().try_write(&Resource {
            ..Default::default()
        })?;
        let s_name = writer.dictionary().try_write_string("scope")?;
        let v = writer.dictionary().try_write_string("1.0")?;
        let scope_ref = writer.dictionary().try_write(&InstrumentationScope {
            name_ref: s_name,
            version_ref: v,
            resource_ref: res_ref,
            ..Default::default()
        })?;

        // Start mock OTLP server
        let (logs_tx, mut logs_rx) = mpsc::channel(10);
        let (metrics_tx, _) = mpsc::channel(1);
        let (trace_tx, _) = mpsc::channel(1);
        let should_fail = Arc::new(Mutex::new(false));

        let mock_service = MockOtlpService {
            logs_tx,
            metrics_tx,
            trace_tx,
            should_fail: should_fail.clone(),
        };

        // Use a random port
        let addr = "127.0.0.1:0";
        let listener = std::net::TcpListener::bind(addr)?;
        let local_addr = listener.local_addr()?;
        drop(listener);

        tokio::spawn(async move {
            Server::builder()
                .add_service(LogsServiceServer::new(mock_service))
                .serve(local_addr)
                .await
        });

        // Small delay to ensure server started
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Write a log event
        writer.events().try_write(&MmapEvent {
            scope_ref,
            time_unix_nano: 123,
            severity_text: "INFO".to_owned(),
            ..Default::default()
        })?;
        let sdk = CollectorSdk {
            reader: OtlpMmapReader::new(file.path())?,
        };
        // We run the full collector here.
        let config = LogSdkConfig {
            log_endpoint: format!("http://{}", local_addr),
            // Speed up test.
            batch_timeout: tokio::time::Duration::from_secs(1),
            ..Default::default()
        };
        let log_pipeline = sdk.send_logs_to(&config);
        // Verify server received the log
        // We need to select our futures together here.
        tokio::select! {
            _ = log_pipeline => {
                panic!("Did not expect logs pipeline to finish!")
            }
            r = logs_rx.recv() => {
                let received = r.expect("Expected log batch, received nothing");
                assert_eq!(received.resource_logs.len(), 1);
                assert_eq!(
                    received.resource_logs[0].scope_logs[0].log_records[0].time_unix_nano,
                    123
                );
            }
        }

        // Test failure
        *should_fail.lock().expect("lock was tainted in test") = true;
        let log_pipeline = sdk.send_logs_to(&config);
        writer.events().try_write(&MmapEvent {
            scope_ref,
            time_unix_nano: 456,
            ..Default::default()
        })?;
        let result = log_pipeline.await;
        assert!(result.is_err());

        Ok(())
    }
}
