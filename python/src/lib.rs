use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use std::sync::{Arc, Mutex};

mod data;
mod sdk;

use sdk::OtlpMmapExporter as InnerExporter;

#[pyclass]
struct OtlpMmapExporter {
    inner: Arc<Mutex<InnerExporter>>,
}

#[pymethods]
impl OtlpMmapExporter {
    fn record_string(&self, string: &str) -> PyResult<usize> {
        let mut inner = self.inner.lock().unwrap();
        inner.record_string(string).map_err(|e: anyhow::Error| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))
    }

    fn create_resource(&self, attributes: &PyDict, _schema_url: Option<&str>) -> PyResult<usize> {
        let attrs = convert_attributes(attributes)?;
        let mut inner = self.inner.lock().unwrap();
        inner.create_resource(attrs, _schema_url.map(|s| s.to_string())).map_err(|e: anyhow::Error| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))
    }

    #[pyo3(signature = (resource_ref, name, version=None, attributes=None))]
    fn create_instrumentation_scope(
        &self,
        resource_ref: usize,
        name: &str,
        version: Option<&str>,
        attributes: Option<&PyDict>,
    ) -> PyResult<usize> {
        let attrs = if let Some(a) = attributes {
            convert_attributes(a)?
        } else {
            Vec::new()
        };
        let mut inner = self.inner.lock().unwrap();
        inner.create_instrumentation_scope(resource_ref, name.to_string(), version.map(|s| s.to_string()), attrs)
            .map_err(|e: anyhow::Error| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))
    }

    fn create_metric_stream(
        &self,
        scope_ref: usize,
        name: &str,
        description: &str,
        unit: &str,
        aggregation: &PyDict,
    ) -> PyResult<usize> {
        let agg = convert_aggregation(aggregation)?;
        let mut inner = self.inner.lock().unwrap();
        inner.create_metric_stream(scope_ref, name.to_string(), description.to_string(), unit.to_string(), Some(agg))
            .map_err(|e: anyhow::Error| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))
    }

    fn record_measurement(
        &self,
        metric_ref: usize,
        attributes: &PyDict,
        time_unix_nano: u64,
        value: f64,
        span_context: Option<&PyDict>,
    ) -> PyResult<()> {
        let attrs = convert_attributes(attributes)?;
        let ctx = if let Some(sc) = span_context {
            Some(convert_span_context(sc)?)
        } else {
            None
        };
        let val = data::measurement::Value::AsDouble(value);
        
        let mut inner = self.inner.lock().unwrap();
        inner.record_measurement(metric_ref, attrs, time_unix_nano, val, ctx)
            .map_err(|e: anyhow::Error| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))
    }

    fn record_event(
        &self,
        scope_ref: usize,
        span_context: &PyAny,
        event_name_ref: usize,
        time_unix_nano: u64,
        attributes: &PyDict,
    ) -> PyResult<()> {
        let attrs = convert_attributes(attributes)?;
        
        let ctx = if span_context.is_none() {
            None
        } else {
            if let Ok(d) = span_context.downcast::<PyDict>() {
                 Some(convert_span_context(d)?)
            } else {
                 None // Or error if not None and not Dict?
            }
        };
        
        let mut inner = self.inner.lock().unwrap();
        inner.record_event(scope_ref, ctx, event_name_ref, time_unix_nano, attrs)
            .map_err(|e: anyhow::Error| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))
    }

    // Changing parent_span_id to &PyAny to avoid Option ambiguity for following args
    fn record_span_start(
        &self,
        scope_ref: usize,
        trace_id: &PyBytes,
        span_id: &PyBytes,
        parent_span_id: &PyAny,
        flags: u32,
        name: &str,
        kind: i32,
        start_time_unix_nano: u64,
        attributes: &PyDict,
    ) -> PyResult<()> {
        let attrs = convert_attributes(attributes)?;
        
        let parent_id = if parent_span_id.is_none() {
            Vec::new()
        } else {
            if let Ok(b) = parent_span_id.extract::<&[u8]>() {
                b.to_vec()
            } else {
                Vec::new() // Or error?
            }
        };

        let mut inner = self.inner.lock().unwrap();
        // Intern attributes first
        let kvs = inner.intern_attributes_public(attrs).map_err(|e: anyhow::Error| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;

        let start_span = data::span_event::StartSpan {
            parent_span_id: parent_id,
            flags,
            name: name.to_string(),
            kind,
            start_time_unix_nano,
            attributes: kvs,
        };
        
        let event = data::span_event::Event::Start(start_span);
        inner.record_span_event(scope_ref, trace_id.as_bytes().to_vec(), span_id.as_bytes().to_vec(), event)
            .map_err(|e: anyhow::Error| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))
    }

    fn record_span_end(
        &self,
        scope_ref: usize,
        trace_id: &PyBytes,
        span_id: &PyBytes,
        end_time_unix_nano: u64,
    ) -> PyResult<()> {
        let end_span = data::span_event::EndSpan {
            end_time_unix_nano,
            status: None, 
        };
        
        let event = data::span_event::Event::End(end_span);
        let mut inner = self.inner.lock().unwrap();
        inner.record_span_event(scope_ref, trace_id.as_bytes().to_vec(), span_id.as_bytes().to_vec(), event)
            .map_err(|e: anyhow::Error| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))
    }
}

#[pyfunction]
fn create_otlp_mmap_exporter(path: &str) -> PyResult<OtlpMmapExporter> {
    let inner = InnerExporter::new(path).map_err(|e: anyhow::Error| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
    Ok(OtlpMmapExporter {
        inner: Arc::new(Mutex::new(inner)),
    })
}

#[pymodule]
fn otlp_mmap_internal(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<OtlpMmapExporter>()?;
    m.add_function(wrap_pyfunction!(create_otlp_mmap_exporter, m)?)?;
    Ok(())
}

// Helpers

fn convert_attributes(dict: &PyDict) -> PyResult<Vec<(String, data::AnyValue)>> {
    let mut attrs = Vec::with_capacity(dict.len());
    for (k, v) in dict {
        let key = k.extract::<String>()?;
        let value = convert_any_value(v)?;
        attrs.push((key, value));
    }
    Ok(attrs)
}

fn convert_any_value(v: &PyAny) -> PyResult<data::AnyValue> {
    if let Ok(s) = v.extract::<String>() {
        Ok(data::AnyValue { value: Some(data::any_value::Value::StringValue(s)) })
    } else if let Ok(b) = v.extract::<bool>() {
        Ok(data::AnyValue { value: Some(data::any_value::Value::BoolValue(b)) })
    } else if let Ok(i) = v.extract::<i64>() {
        Ok(data::AnyValue { value: Some(data::any_value::Value::IntValue(i)) })
    } else if let Ok(f) = v.extract::<f64>() {
        Ok(data::AnyValue { value: Some(data::any_value::Value::DoubleValue(f)) })
    } else {
        if let Ok(b) = v.extract::<&[u8]>() {
            Ok(data::AnyValue { value: Some(data::any_value::Value::BytesValue(b.to_vec())) })
        } else {
             Ok(data::AnyValue { value: None })
        }
    }
}

fn convert_aggregation(dict: &PyDict) -> PyResult<data::metric_ref::Aggregation> {
    if dict.contains("gauge")? {
        Ok(data::metric_ref::Aggregation::Gauge(data::metric_ref::Gauge {}))
    } else if let Some(sum_dict) = dict.get_item("sum")? {
         let d = sum_dict.downcast::<PyDict>()?;
         // Safely extract fields
         let temp = if let Some(i) = d.get_item("aggregation_temporality")? { i.extract::<i32>()? } else { 0 };
         let mono = if let Some(i) = d.get_item("is_monotonic")? { i.extract::<bool>()? } else { false };
         Ok(data::metric_ref::Aggregation::Sum(data::metric_ref::Sum {
             aggregation_temporality: temp,
             is_monotonic: mono,
         }))
    } else if let Some(hist_dict) = dict.get_item("histogram")? {
        let d = hist_dict.downcast::<PyDict>()?;
        let temp = if let Some(i) = d.get_item("aggregation_temporality")? { i.extract::<i32>()? } else { 0 };
        let bounds = if let Some(i) = d.get_item("bucket_boundaries")? { i.extract::<Vec<f64>>()? } else { Vec::new() };
        Ok(data::metric_ref::Aggregation::Histogram(data::metric_ref::Histogram {
             aggregation_temporality: temp,
             bucket_boundares: bounds,
        }))
    } else if let Some(exp_dict) = dict.get_item("exp_histogram")? {
        let d = exp_dict.downcast::<PyDict>()?;
        let temp = if let Some(i) = d.get_item("aggregation_temporality")? { i.extract::<i32>()? } else { 0 };
        let buckets = if let Some(i) = d.get_item("max_buckets")? { i.extract::<i64>()? } else { 0 };
        let scale = if let Some(i) = d.get_item("max_scale")? { i.extract::<i64>()? } else { 0 };
        Ok(data::metric_ref::Aggregation::ExpHist(data::metric_ref::ExponentialHistogram {
             aggregation_temporality: temp,
             max_buckets: buckets,
             max_scale: scale,
        }))
    } else {
        Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("Unknown aggregation type"))
    }
}

fn convert_span_context(dict: &PyDict) -> PyResult<data::SpanContext> {
    let trace_id = if let Some(item) = dict.get_item("trace_id")? {
        item.extract::<&[u8]>()?.to_vec()
    } else {
        Vec::new()
    };
    let span_id = if let Some(item) = dict.get_item("span_id")? {
        item.extract::<&[u8]>()?.to_vec()
    } else {
        Vec::new()
    };
    let flags = if let Some(item) = dict.get_item("flags")? {
        item.extract::<u32>()?
    } else {
        0
    };
    Ok(data::SpanContext {
        trace_id,
        span_id,
        flags,
    })
}
