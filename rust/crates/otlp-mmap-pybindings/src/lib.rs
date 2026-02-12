use std::path::Path;
use std::sync::Arc;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use otlp_mmap_core::{OtlpMmapConfig, OtlpMmapWriter};
use scc::HashIndex;

#[pyclass]
struct OtlpMmapExporter {
    writer: Arc<WriterHelper>,
}

#[pymethods]
impl OtlpMmapExporter {
    fn record_string(&self, string: &str) -> PyResult<i64> {        
        Ok(self.writer.intern_string(string)?)
    }
    fn create_resource(&self, attributes:  &Bound<'_, PyDict>, schema_url: Option<&str>) -> PyResult<i64> {
        self.writer.intern_resource(attributes, schema_url)
    }
    #[pyo3(signature = (resource_ref, name, version=None, attributes=None))]
    fn create_instrumentation_scope(
        &self,
        resource_ref: usize,
        name: &str,
        version: Option<&str>,
        attributes: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<usize> {
        todo!()
    }
    fn create_metric_stream(
        &self,
        scope_ref: usize,
        name: &str,
        description: &str,
        unit: &str,
        aggregation: &Bound<'_, PyDict>,
    ) -> PyResult<usize> {
        todo!()
    }
    fn record_measurement(
        &self,
        metric_ref: usize,
        attributes: &Bound<'_, PyDict>,
        time_unix_nano: u64,
        value: f64,
        span_context: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        todo!()
    }

    fn record_event(
        &self,
        scope_ref: usize,
        span_context: &Bound<'_, PyAny>,
        event_name_ref: usize,
        time_unix_nano: u64,
        attributes: &Bound<'_, PyDict>,
    ) -> PyResult<()> {
        todo!()
    }

    // Changing parent_span_id to &PyAny to avoid Option ambiguity for following args
    fn record_span_start(
        &self,
        scope_ref: usize,
        trace_id: &Bound<'_, PyBytes>,
        span_id: &Bound<'_, PyBytes>,
        parent_span_id: &Bound<'_, PyAny>,
        flags: u32,
        name: &str,
        kind: i32,
        start_time_unix_nano: u64,
        attributes: &Bound<'_, PyDict>,
    ) -> PyResult<()> {
        todo!()
    }

    fn record_span_end(
        &self,
        scope_ref: usize,
        trace_id: &Bound<'_, PyBytes>,
        span_id: &Bound<'_, PyBytes>,
        end_time_unix_nano: u64,
    ) -> PyResult<()> {
        todo!()
    }
}

#[pyfunction]
fn create_otlp_mmap_exporter(path: &str) -> PyResult<OtlpMmapExporter> {
    // TODO - Configuration from python.
    let config = OtlpMmapConfig::default();
    let writer = OtlpMmapWriter::new(Path::new(path), &config).map_err(core_to_py_err)?;
    let key_cache = HashIndex::new();
    Ok(OtlpMmapExporter { writer: Arc::new(WriterHelper { writer, key_cache }) })
}

#[pymodule]
mod otlp_mmap_internal {
    #[pymodule_export]
    use super::OtlpMmapExporter;
    #[pymodule_export]
    use super::create_otlp_mmap_exporter;
}

/// Conversion from core errors to py errors.
fn core_to_py_err(e: otlp_mmap_core::Error) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string())
}



/// Helper for writing values to OTLP-MMAP that will attempt to re-use/cache dictionary items across
/// various SDK calls.
struct WriterHelper {
    /// Writer of values to the OTLP-MMAP file.
    writer: OtlpMmapWriter,
    /// Cache of previously written keys in the dictionary.
    key_cache: HashIndex<String, i64>,
    // TODO - Resoure cache?
    // TODO - InstrumentationScope cache?
    // TODO - Metric cache?
}

impl WriterHelper {
    fn intern_string(&self, value: &str) -> PyResult<i64> {
        if let Some(idx) = self.key_cache.get_sync(value) {
            return Ok(*idx.get());
        }
        let idx = self.writer.dictionary().try_write_string(&value).map_err(core_to_py_err)?;
        let _ = self.key_cache.insert_sync(value.to_owned(), idx);
        Ok(idx)
    }

     fn intern_resource(&self, attributes:  &Bound<'_, PyDict>, _schema_url: Option<&str>) -> PyResult<i64> {
        let attributes = self.convert_attributes(attributes)?;
        let resource = otlp_mmap_protocol::Resource {
             attributes,
             dropped_attributes_count: 0,
        };
        // TODO - use cache.
        let result = self.writer.dictionary().try_write(&resource).map_err(core_to_py_err)?;
        Ok(result)
    }

    /// Converts a python dictionary into OTLP-MMAP KeyValueRefs.
    fn convert_attributes(&self, dict: &Bound<'_, PyDict>) -> PyResult<Vec<otlp_mmap_protocol::KeyValueRef>> {
        let mut attrs = Vec::with_capacity(dict.len());
        for (k, v) in dict {
            let key = k.extract::<String>()?;
            let key_ref = self.intern_string(&key)?;
            let value = self.convert_any_value(&v)?;
            attrs.push(otlp_mmap_protocol::KeyValueRef { key_ref, value: Some(value) });
        }
        Ok(attrs)
    }

    /// Converts a python any into an OTLP-MMAP AnyValue.
    fn convert_any_value(&self, v:  &Bound<'_, PyAny>) -> PyResult<otlp_mmap_protocol::AnyValue> {
        // TODO - We should handle complex values.
        if let Ok(s) = v.extract::<String>() {
            Ok(otlp_mmap_protocol::AnyValue { value: Some(otlp_mmap_protocol::any_value::Value::StringValue(s)) })
        } else if let Ok(b) = v.extract::<bool>() {
            Ok(otlp_mmap_protocol::AnyValue { value: Some(otlp_mmap_protocol::any_value::Value::BoolValue(b)) })
        } else if let Ok(i) = v.extract::<i64>() {
            Ok(otlp_mmap_protocol::AnyValue { value: Some(otlp_mmap_protocol::any_value::Value::IntValue(i)) })
        } else if let Ok(f) = v.extract::<f64>() {
            Ok(otlp_mmap_protocol::AnyValue { value: Some(otlp_mmap_protocol::any_value::Value::DoubleValue(f)) })
        } else {
            if let Ok(b) = v.extract::<&[u8]>() {
                Ok(otlp_mmap_protocol::AnyValue { value: Some(otlp_mmap_protocol::any_value::Value::BytesValue(b.to_vec())) })
            } else {
                Ok(otlp_mmap_protocol::AnyValue { value: None })
            }
        }
    }
}