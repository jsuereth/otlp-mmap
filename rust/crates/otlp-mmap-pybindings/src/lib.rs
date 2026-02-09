use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

#[pyclass]
struct OtlpMmapExporter {}

#[pymethods]
impl OtlpMmapExporter {
    fn record_string(&self, string: &str) -> PyResult<usize> {
        todo!()
    }
    fn create_resource(&self, attributes: &PyDict, _schema_url: Option<&str>) -> PyResult<usize> {
        todo!()
    }
    #[pyo3(signature = (resource_ref, name, version=None, attributes=None))]
    fn create_instrumentation_scope(
        &self,
        resource_ref: usize,
        name: &str,
        version: Option<&str>,
        attributes: Option<&PyDict>,
    ) -> PyResult<usize> {
        todo!()
    }
    fn create_metric_stream(
        &self,
        scope_ref: usize,
        name: &str,
        description: &str,
        unit: &str,
        aggregation: &PyDict,
    ) -> PyResult<usize> {
        todo!()
    }
    fn record_measurement(
        &self,
        metric_ref: usize,
        attributes: &PyDict,
        time_unix_nano: u64,
        value: f64,
        span_context: Option<&PyDict>,
    ) -> PyResult<()> {
        todo!()
    }

    fn record_event(
        &self,
        scope_ref: usize,
        span_context: &PyAny,
        event_name_ref: usize,
        time_unix_nano: u64,
        attributes: &PyDict,
    ) -> PyResult<()> {
        todo!()
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
        todo!()
    }

    fn record_span_end(
        &self,
        scope_ref: usize,
        trace_id: &PyBytes,
        span_id: &PyBytes,
        end_time_unix_nano: u64,
    ) -> PyResult<()> {
        todo!()
    }
}

#[pyfunction]
fn create_otlp_mmap_exporter(path: &str) -> PyResult<OtlpMmapExporter> {
    todo!()
}

#[pymodule]
fn otlp_mmap_internal(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<OtlpMmapExporter>()?;
    // m.add_class::<MmapReader>()?;
    m.add_function(wrap_pyfunction!(create_otlp_mmap_exporter, m)?)?;
    Ok(())
}
