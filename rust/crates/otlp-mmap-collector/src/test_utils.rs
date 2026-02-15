//! Utilities/mocks for testing.

use crate::{AsyncEventQueue, AttributeLookup, Error, SdkLookup};
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsService;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsService;
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceService;
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use otlp_mmap_core::PartialScope;
use otlp_mmap_protocol::{any_value::Value, AnyValue, KeyValueRef, MetricRef};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

/// Mock for performing lookups on OTLP-MMAP dictionary.
pub struct MockSdkLookup {
    pub strings: HashMap<i64, String>,
    pub scopes: HashMap<i64, PartialScope>,
    pub resources: HashMap<i64, opentelemetry_proto::tonic::resource::v1::Resource>,
    pub metrics: HashMap<i64, MetricRef>,
    pub should_error: bool,
}

impl MockSdkLookup {
    pub fn new() -> Self {
        Self {
            strings: HashMap::new(),
            scopes: HashMap::new(),
            resources: HashMap::new(),
            metrics: HashMap::new(),
            should_error: false,
        }
    }
}

impl AttributeLookup for MockSdkLookup {
    fn try_convert_attribute(
        &self,
        kv: KeyValueRef,
    ) -> Result<opentelemetry_proto::tonic::common::v1::KeyValue, Error> {
        if self.should_error {
            return Err(
                otlp_mmap_core::Error::NotFoundInDictionary("intentional".to_owned(), 0).into(),
            );
        }
        let key = self
            .strings
            .get(&kv.key_ref)
            .cloned()
            .unwrap_or_else(|| format!("key_{}", kv.key_ref));
        let value = if let Some(av) = kv.value {
            self.try_convert_anyvalue(av)?
        } else {
            None
        };
        Ok(opentelemetry_proto::tonic::common::v1::KeyValue { key, value })
    }
}

impl SdkLookup for MockSdkLookup {
    fn try_lookup_scope(&self, instrumentation_scope_ref: i64) -> Result<PartialScope, Error> {
        if self.should_error {
            return Err(
                otlp_mmap_core::Error::NotFoundInDictionary("intentional".to_owned(), 0).into(),
            );
        }
        self.scopes
            .get(&instrumentation_scope_ref)
            .cloned()
            .ok_or_else(|| {
                otlp_mmap_core::Error::NotFoundInDictionary(
                    "scope".to_owned(),
                    instrumentation_scope_ref,
                )
                .into()
            })
    }

    fn try_lookup_resource(
        &self,
        resource_ref: i64,
    ) -> Result<opentelemetry_proto::tonic::resource::v1::Resource, Error> {
        if self.should_error {
            return Err(
                otlp_mmap_core::Error::NotFoundInDictionary("intentional".to_owned(), 0).into(),
            );
        }
        self.resources.get(&resource_ref).cloned().ok_or_else(|| {
            otlp_mmap_core::Error::NotFoundInDictionary("resource".to_owned(), resource_ref).into()
        })
    }

    fn try_read_string(&self, string_ref: i64) -> Result<String, Error> {
        if self.should_error {
            return Err(
                otlp_mmap_core::Error::NotFoundInDictionary("intentional".to_owned(), 0).into(),
            );
        }
        self.strings.get(&string_ref).cloned().ok_or_else(|| {
            otlp_mmap_core::Error::NotFoundInDictionary("string".to_owned(), string_ref).into()
        })
    }

    fn try_lookup_metric(&self, metric_ref: i64) -> Result<MetricRef, Error> {
        if self.should_error {
            return Err(
                otlp_mmap_core::Error::NotFoundInDictionary("intentional".to_owned(), 0).into(),
            );
        }
        self.metrics.get(&metric_ref).cloned().ok_or_else(|| {
            otlp_mmap_core::Error::NotFoundInDictionary("metric".to_owned(), metric_ref).into()
        })
    }

    fn try_convert_anyvalue(
        &self,
        value: AnyValue,
    ) -> Result<Option<opentelemetry_proto::tonic::common::v1::AnyValue>, Error> {
        if self.should_error {
            return Err(
                otlp_mmap_core::Error::NotFoundInDictionary("intentional".to_owned(), 0).into(),
            );
        }
        match value.value {
            Some(Value::StringValue(s)) => {
                Ok(Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s),
                    ),
                }))
            }
            Some(Value::BoolValue(b)) => {
                Ok(Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(b),
                    ),
                }))
            }
            Some(Value::IntValue(i)) => {
                Ok(Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i),
                    ),
                }))
            }
            Some(Value::DoubleValue(d)) => {
                Ok(Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d),
                    ),
                }))
            }
            Some(Value::BytesValue(b)) => {
                Ok(Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::BytesValue(b),
                    ),
                }))
            }
            Some(Value::ValueRef(r)) => {
                let s = self.try_read_string(r)?;
                Ok(Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s),
                    ),
                }))
            }
            Some(Value::ArrayValue(av)) => {
                let mut values = Vec::new();
                for val in av.values {
                    if let Some(v) = self.try_convert_anyvalue(val)? {
                        values.push(v);
                    }
                }
                Ok(Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::ArrayValue(
                            opentelemetry_proto::tonic::common::v1::ArrayValue { values },
                        ),
                    ),
                }))
            }
            Some(Value::KvlistValue(kvl)) => {
                let mut values = Vec::new();
                for kv in kvl.values {
                    values.push(self.try_convert_attribute(kv)?);
                }
                Ok(Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::KvlistValue(
                            opentelemetry_proto::tonic::common::v1::KeyValueList { values },
                        ),
                    ),
                }))
            }
            _ => Ok(None),
        }
    }
}

/// Mock for pulling Events from an OTLP-MMAP ringbuffer.
pub struct TestEventQueue<T> {
    index: Mutex<usize>,
    events: Vec<T>,
}

impl<T> TestEventQueue<T> {
    pub fn new<E: Into<Vec<T>>>(events: E) -> Self {
        Self {
            index: Mutex::new(0),
            events: events.into(),
        }
    }
}

impl<T: prost::Message + Default + Clone + Sync + 'static> AsyncEventQueue<T>
    for TestEventQueue<T>
{
    async fn try_read_next(&self) -> Result<T, Error> {
        let mut idx = self.index.lock().await;
        if *idx < self.events.len() {
            let real_idx: usize = *idx;
            *idx += 1;
            Ok(self.events[real_idx].to_owned())
        } else {
            // Return an error that we can catch to stop looping if needed,
            // though usually we use timeouts.
            Err(otlp_mmap_core::Error::NotFoundInDictionary("queue".to_owned(), *idx as i64).into())
        }
    }
}

/// Mock for sending OTLP data.
pub struct MockOtlpService {
    pub logs_tx: mpsc::Sender<ExportLogsServiceRequest>,
    pub metrics_tx: mpsc::Sender<ExportMetricsServiceRequest>,
    pub trace_tx: mpsc::Sender<ExportTraceServiceRequest>,
    pub should_fail: Arc<std::sync::Mutex<bool>>,
}

#[tonic::async_trait]
impl LogsService for MockOtlpService {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        if *self
            .should_fail
            .lock()
            .expect("Lock should not be poisoned")
        {
            return Err(Status::internal("intentional failure"));
        }
        let _ = self.logs_tx.send(request.into_inner()).await;
        Ok(Response::new(ExportLogsServiceResponse {
            partial_success: None,
        }))
    }
}

#[tonic::async_trait]
impl MetricsService for MockOtlpService {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        if *self
            .should_fail
            .lock()
            .expect("Lock should not be poisoned")
        {
            return Err(Status::internal("intentional failure"));
        }
        let _ = self.metrics_tx.send(request.into_inner()).await;
        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
        }))
    }
}

#[tonic::async_trait]
impl TraceService for MockOtlpService {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        if *self
            .should_fail
            .lock()
            .expect("Lock should not be poisoned")
        {
            return Err(Status::internal("intentional failure"));
        }
        let _ = self.trace_tx.send(request.into_inner()).await;
        Ok(Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}
