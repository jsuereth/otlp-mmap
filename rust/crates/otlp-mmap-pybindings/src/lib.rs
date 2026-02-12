use otlp_mmap_core::{OtlpMmapConfig, OtlpMmapReader};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};

use crate::sdk::SdkWriter;

mod sdk;

#[pyclass]
struct OtlpMmapExporter {
    writer: Arc<SdkWriter>,
}

#[pymethods]
impl OtlpMmapExporter {
    fn record_string(&self, string: &str) -> PyResult<i64> {
        Ok(self.writer.intern_string(string)?)
    }
    fn create_resource(
        &self,
        attributes: &Bound<'_, PyDict>,
        schema_url: Option<&str>,
    ) -> PyResult<i64> {
        self.writer.intern_resource(attributes, schema_url)
    }
    #[pyo3(signature = (resource_ref, name, version=None, attributes=None))]
    fn create_instrumentation_scope(
        &self,
        resource_ref: i64,
        name: &str,
        version: Option<&str>,
        attributes: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<i64> {
        self.writer
            .intern_instrumentation_scope(resource_ref, name, version, attributes)
    }
    fn create_metric_stream(
        &self,
        scope_ref: i64,
        name: &str,
        description: &str,
        unit: &str,
        aggregation: &Bound<'_, PyDict>,
    ) -> PyResult<i64> {
        self.writer
            .intern_metric_stream(scope_ref, name, description, unit, aggregation)
    }
    fn record_measurement(
        &self,
        metric_ref: i64,
        attributes: &Bound<'_, PyDict>,
        time_unix_nano: u64,
        value: f64,
        span_context: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        let kvs = self.writer.convert_attributes(attributes)?;
        let ctx = if let Some(sc) = span_context {
            Some(convert_span_context(sc)?)
        } else {
            None
        };
        let val = otlp_mmap_protocol::measurement::Value::AsDouble(value);
        let m = otlp_mmap_protocol::Measurement {
            metric_ref: metric_ref,
            attributes: kvs,
            time_unix_nano,
            span_context: ctx,
            value: Some(val),
        };
        self.writer.record_measurement(attributes.py(), m)
    }

    fn record_event(
        &self,
        scope_ref: i64,
        span_context: &Bound<'_, PyAny>,
        event_name: &str,
        time_unix_nano: u64,
        severity_number: i32,
        severity_text: &str,
        attributes: &Bound<'_, PyDict>,
    ) -> PyResult<()> {
        let kvs = self.writer.convert_attributes(attributes)?;
        // TODO - use offset of 0 if string is empty?
        let event_name_ref = self.writer.intern_string(event_name)?;
        let ctx = if span_context.is_none() {
            None
        } else {
            if let Ok(d) = span_context.cast::<PyDict>() {
                Some(convert_span_context(d)?)
            } else {
                None // Or error if not None and not Dict?
            }
        };
        let e = otlp_mmap_protocol::Event {
            scope_ref,
            time_unix_nano,
            event_name_ref,
            span_context: ctx,
            attributes: kvs,
            // TODO - add these into method argument.
            severity_number,
            severity_text: severity_text.to_owned(),
            body: None,
        };
        self.writer.record_event(attributes.py(), e)
    }

    // Changing parent_span_id to &PyAny to avoid Option ambiguity for following args
    fn record_span_start(
        &self,
        scope_ref: i64,
        trace_id: &Bound<'_, PyBytes>,
        span_id: &Bound<'_, PyBytes>,
        parent_span_id: &Bound<'_, PyAny>,
        flags: u32,
        name: &str,
        kind: i32,
        start_time_unix_nano: u64,
        attributes: &Bound<'_, PyDict>,
    ) -> PyResult<()> {
        let attributes = self.writer.convert_attributes(attributes)?;
        // TODO - better validation of ids.
        let parent_id = if parent_span_id.is_none() {
            Vec::new()
        } else {
            if let Ok(b) = parent_span_id.extract::<&[u8]>() {
                b.to_vec()
            } else {
                Vec::new() // Or error?
            }
        };
        let start_span = otlp_mmap_protocol::span_event::StartSpan {
            parent_span_id: parent_id,
            flags,
            name: name.to_string(),
            kind,
            start_time_unix_nano,
            attributes,
        };

        let event = otlp_mmap_protocol::span_event::Event::Start(start_span);
        self.writer.record_span_event(
            trace_id.py(),
            scope_ref,
            trace_id.extract()?,
            span_id.extract()?,
            event,
        )
    }

    fn record_span_end(
        &self,
        scope_ref: i64,
        trace_id: &Bound<'_, PyBytes>,
        span_id: &Bound<'_, PyBytes>,
        end_time_unix_nano: u64,
    ) -> PyResult<()> {
        let end_span = otlp_mmap_protocol::span_event::EndSpan {
            end_time_unix_nano,
            status: None,
        };
        let event = otlp_mmap_protocol::span_event::Event::End(end_span);
        self.writer.record_span_event(
            trace_id.py(),
            scope_ref,
            trace_id.extract()?,
            span_id.extract()?,
            event,
        )
    }
}

fn convert_span_context(dict: &Bound<'_, PyDict>) -> PyResult<otlp_mmap_protocol::SpanContext> {
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
    Ok(otlp_mmap_protocol::SpanContext {
        trace_id,
        span_id,
        flags,
    })
}

#[pyfunction]
fn create_otlp_mmap_exporter(path: &str) -> PyResult<OtlpMmapExporter> {
    // TODO - Configuration from python.
    let config = OtlpMmapConfig::default();
    Ok(OtlpMmapExporter {
        writer: Arc::new(SdkWriter::new(Path::new(path), &config)?),
    })
}

/// An inefficient reader implementation used for testing Python SDK implementation only.
#[pyclass]
struct TestOtlpMmapReader {
    reader: Arc<Mutex<OtlpMmapReader>>,
}

#[pymethods]
impl TestOtlpMmapReader {
    fn read_string(&self, idx: i64) -> PyResult<String> {
        let reader = self.reader.lock().map_err(poison_to_py_err)?;
        reader
            .dictionary()
            .try_lookup_string(idx)
            .map_err(core_to_py_err)
    }
    fn read_resource<'a>(&self, py: Python<'a>, idx: i64) -> PyResult<Bound<'a, PyDict>> {
        let reader = self.reader.lock().map_err(poison_to_py_err)?;
        // TODO - convert proto to resource?
        let result = reader
            .dictionary()
            .try_lookup_resource(idx)
            .map_err(core_to_py_err)?;
        let dict = PyDict::new(py);
        // TODO - Add attributes
        dict.set_item("dropped_attributes_count", result.dropped_attributes_count)?;
        Ok(dict)
    }
    fn read_scope<'a>(&self, py: Python<'a>, idx: i64) -> PyResult<Bound<'a, PyDict>> {
        let reader = self.reader.lock().map_err(poison_to_py_err)?;
        // TODO - convert proto to resource?
        let result = reader
            .dictionary()
            .try_lookup_scope(idx)
            .map_err(core_to_py_err)?;
        let dict = PyDict::new(py);
        dict.set_item("resource_ref", result.resource_ref)?;
        dict.set_item("name", result.scope.name)?;
        dict.set_item("version", result.scope.version)?;
        dict.set_item(
            "dropped_attributes_count",
            result.scope.dropped_attributes_count,
        )?;
        // TODO - Add attributes
        Ok(dict)
    }
    fn read_metric<'a>(&self, py: Python<'a>, idx: i64) -> PyResult<Bound<'a, PyDict>> {
        let reader = self.reader.lock().map_err(poison_to_py_err)?;
        // TODO - convert proto to resource?
        let result = reader
            .dictionary()
            .try_lookup_metric_stream(idx)
            .map_err(core_to_py_err)?;
        let dict = PyDict::new(py);
        dict.set_item(
            "instrumentation_scope_ref",
            result.instrumentation_scope_ref,
        )?;
        dict.set_item("name", result.name)?;
        dict.set_item("name", result.description)?;
        dict.set_item("unit", result.unit)?;
        // TODO - Add aggregation
        Ok(dict)
    }

    fn read_measurement<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyDict>> {
        let reader = self.reader.lock().map_err(poison_to_py_err)?;
        // TODO - spin-lock read.
        let dict = PyDict::new(py);
        if let Some(result) = reader.metrics().try_read().map_err(core_to_py_err)? {
            dict.set_item("metric_ref", result.metric_ref)?;
            dict.set_item("time_unix_nano", result.time_unix_nano)?;
            // TODO - attributes
            // TODO - span context
        }
        Ok(dict)
    }
}

#[pyfunction]
fn create_test_otlp_mmap_reader(path: &str) -> PyResult<TestOtlpMmapReader> {
    Ok(TestOtlpMmapReader {
        reader: Arc::new(Mutex::new(
            OtlpMmapReader::new(Path::new(path)).map_err(core_to_py_err)?,
        )),
    })
}

#[pymodule]
mod otlp_mmap_internal {
    #[pymodule_export]
    use super::create_otlp_mmap_exporter;
    #[pymodule_export]
    use super::create_test_otlp_mmap_reader;
    #[pymodule_export]
    use super::OtlpMmapExporter;
    #[pymodule_export]
    use super::TestOtlpMmapReader;
}

/// Conversion from core errors to py errors.
pub(crate) fn core_to_py_err(e: otlp_mmap_core::Error) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string())
}

pub(crate) fn poison_to_py_err<T>(e: PoisonError<T>) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string())
}
