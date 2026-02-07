use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

#[pyfunction]
fn record_string(_string: &str) -> PyResult<usize> {
    todo!()
}

#[pyfunction]
fn create_resource(_attributes: &PyDict, _schema_url: Option<&str>) -> PyResult<usize> {
    todo!()
}

#[pyfunction]
#[pyo3(signature = (resource_ref, name, version=None, attributes=None))]
fn create_instrumentation_scope(
    resource_ref: usize,
    name: &str,
    version: Option<&str>,
    attributes: Option<&PyDict>,
) -> PyResult<usize> {
    todo!()
}

#[pyfunction]
fn create_metric_stream(
    _instrumentation_scope_ref: usize,
    _name: &str,
    _description: &str,
    _unit: &str,
    _aggregation: &PyDict,
) -> PyResult<usize> {
    todo!()
}

#[pyfunction]
fn record_measurement(
    _metric_ref: usize,
    _attributes: &PyDict,
    _time_unix_nano: u64,
    _value: f64,
    _span_context: &PyDict,
) -> PyResult<()> {
    todo!()
}

#[pyfunction]
fn record_event(
    _instrumentation_scope_ref: usize,
    _span_context: &PyDict,
    _event_name_ref: usize,
    _time_unix_nano: u64,
    _attributes: &PyDict,
) -> PyResult<()> {
    todo!()
}

#[pyfunction]
fn record_span_start(
    _instrumentation_scope_ref: usize,
    _trace_id: &PyBytes,
    _span_id: &PyBytes,
    _parent_span_id: &PyBytes,
    _flags: u32,
    _name: &str,
    _kind: i32,
    _start_time_unix_nano: u64,
    _attributes: &PyDict,
) -> PyResult<()> {
    todo!()
}

#[pyfunction]
fn record_span_end(
    _instrumentation_scope_ref: usize,
    _trace_id: &PyBytes,
    _span_id: &PyBytes,
    _end_time_unix_nano: u64,
) -> PyResult<()> {
    todo!()
}

#[pymodule]
fn otlp_mmap(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(record_string, m)?)?;
    m.add_function(wrap_pyfunction!(create_resource, m)?)?;
    m.add_function(wrap_pyfunction!(create_instrumentation_scope, m)?)?;
    m.add_function(wrap_pyfunction!(create_metric_stream, m)?)?;
    m.add_function(wrap_pyfunction!(record_measurement, m)?)?;
    m.add_function(wrap_pyfunction!(record_event, m)?)?;
    m.add_function(wrap_pyfunction!(record_span_start, m)?)?;
    m.add_function(wrap_pyfunction!(record_span_end, m)?)?;
    Ok(())
}