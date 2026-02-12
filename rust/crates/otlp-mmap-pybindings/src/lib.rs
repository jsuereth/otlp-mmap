use otlp_mmap_core::{OtlpMmapConfig, OtlpMmapWriter, RingBufferWriter};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use scc::HashIndex;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

#[pyclass]
struct OtlpMmapExporter {
    writer: Arc<WriterHelper>,
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
    let writer = OtlpMmapWriter::new(Path::new(path), &config).map_err(core_to_py_err)?;
    let key_cache = HashIndex::new();
    Ok(OtlpMmapExporter {
        writer: Arc::new(WriterHelper { writer, key_cache }),
    })
}

#[pymodule]
mod otlp_mmap_internal {
    #[pymodule_export]
    use super::create_otlp_mmap_exporter;
    #[pymodule_export]
    use super::OtlpMmapExporter;
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
    /// Helper to record a span event into the ring buffer.
    fn record_span_event(
        &self,
        py: Python<'_>,
        scope_ref: i64,
        trace_id: Vec<u8>,
        span_id: Vec<u8>,
        event: otlp_mmap_protocol::span_event::Event,
    ) -> PyResult<()> {
        let s = otlp_mmap_protocol::SpanEvent {
            scope_ref: scope_ref,
            trace_id,
            span_id,
            event: Some(event),
        };
        spin_lock_write(py, self.writer.spans(), &s)
    }

    /// spin-lock write of measurement to our ring buffer.
    fn record_measurement(
        &self,
        py: Python<'_>,
        measurement: otlp_mmap_protocol::Measurement,
    ) -> PyResult<()> {
        spin_lock_write(py, self.writer.measurements(), &measurement)
    }

    /// spin-lock write of events to our ring buffer.
    fn record_event(&self, py: Python<'_>, event: otlp_mmap_protocol::Event) -> PyResult<()> {
        spin_lock_write(py, self.writer.events(), &event)
    }

    fn intern_string(&self, value: &str) -> PyResult<i64> {
        if let Some(idx) = self.key_cache.get_sync(value) {
            return Ok(*idx.get());
        }
        let idx = self
            .writer
            .dictionary()
            .try_write_string(&value)
            .map_err(core_to_py_err)?;
        let _ = self.key_cache.insert_sync(value.to_owned(), idx);
        Ok(idx)
    }

    fn intern_resource(
        &self,
        attributes: &Bound<'_, PyDict>,
        _schema_url: Option<&str>,
    ) -> PyResult<i64> {
        let attributes = self.convert_attributes(attributes)?;
        let resource = otlp_mmap_protocol::Resource {
            attributes,
            dropped_attributes_count: 0,
        };
        // TODO - use cache.
        let result = self
            .writer
            .dictionary()
            .try_write(&resource)
            .map_err(core_to_py_err)?;
        Ok(result)
    }

    fn intern_instrumentation_scope(
        &self,
        resource_ref: i64,
        name: &str,
        version: Option<&str>,
        attributes: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<i64> {
        let kvs = if let Some(a) = attributes {
            self.convert_attributes(a)?
        } else {
            Vec::new()
        };
        let name_ref = self.intern_string(name)?;
        let version_ref = if let Some(v) = version {
            self.intern_string(v)?
        } else {
            0
        };
        let scope = otlp_mmap_protocol::InstrumentationScope {
            name_ref,
            version_ref,
            attributes: kvs,
            dropped_attributes_count: 0,
            resource_ref,
        };
        // TODO - use cache.
        self.writer
            .dictionary()
            .try_write(&scope)
            .map_err(core_to_py_err)
    }
    fn intern_metric_stream(
        &self,
        scope_ref: i64,
        name: &str,
        description: &str,
        unit: &str,
        aggregation: &Bound<'_, PyDict>,
    ) -> PyResult<i64> {
        let agg = convert_aggregation(aggregation)?;
        let metric = otlp_mmap_protocol::MetricRef {
            name: name.to_owned(),
            description: description.to_owned(),
            unit: unit.to_owned(),
            instrumentation_scope_ref: scope_ref,
            aggregation: Some(agg),
        };
        // TODO - use cache.
        let result = self
            .writer
            .dictionary()
            .try_write(&metric)
            .map_err(core_to_py_err)?;
        Ok(result)
    }

    /// Converts a python dictionary into OTLP-MMAP KeyValueRefs.
    fn convert_attributes(
        &self,
        dict: &Bound<'_, PyDict>,
    ) -> PyResult<Vec<otlp_mmap_protocol::KeyValueRef>> {
        let mut attrs = Vec::with_capacity(dict.len());
        for (k, v) in dict {
            let key = k.extract::<String>()?;
            let key_ref = self.intern_string(&key)?;
            let value = self.convert_any_value(&v)?;
            attrs.push(otlp_mmap_protocol::KeyValueRef {
                key_ref,
                value: Some(value),
            });
        }
        Ok(attrs)
    }

    /// Converts a python any into an OTLP-MMAP AnyValue.
    fn convert_any_value(&self, v: &Bound<'_, PyAny>) -> PyResult<otlp_mmap_protocol::AnyValue> {
        // TODO - We should handle complex values.
        if let Ok(s) = v.extract::<String>() {
            Ok(otlp_mmap_protocol::AnyValue {
                value: Some(otlp_mmap_protocol::any_value::Value::StringValue(s)),
            })
        } else if let Ok(b) = v.extract::<bool>() {
            Ok(otlp_mmap_protocol::AnyValue {
                value: Some(otlp_mmap_protocol::any_value::Value::BoolValue(b)),
            })
        } else if let Ok(i) = v.extract::<i64>() {
            Ok(otlp_mmap_protocol::AnyValue {
                value: Some(otlp_mmap_protocol::any_value::Value::IntValue(i)),
            })
        } else if let Ok(f) = v.extract::<f64>() {
            Ok(otlp_mmap_protocol::AnyValue {
                value: Some(otlp_mmap_protocol::any_value::Value::DoubleValue(f)),
            })
        } else {
            if let Ok(b) = v.extract::<&[u8]>() {
                Ok(otlp_mmap_protocol::AnyValue {
                    value: Some(otlp_mmap_protocol::any_value::Value::BytesValue(b.to_vec())),
                })
            } else {
                Ok(otlp_mmap_protocol::AnyValue { value: None })
            }
        }
    }
}

/// Helper method to convert our ditionary-based aggregation definition syntax in python to the proto.
fn convert_aggregation(
    dict: &Bound<'_, PyDict>,
) -> PyResult<otlp_mmap_protocol::metric_ref::Aggregation> {
    if dict.contains("gauge")? {
        Ok(otlp_mmap_protocol::metric_ref::Aggregation::Gauge(
            otlp_mmap_protocol::metric_ref::Gauge {},
        ))
    } else if let Some(sum_dict) = dict.get_item("sum")? {
        let d = sum_dict.cast::<PyDict>()?;
        // Safely extract fields
        let temp = if let Some(i) = d.get_item("aggregation_temporality")? {
            i.extract::<i32>()?
        } else {
            0
        };
        let mono = if let Some(i) = d.get_item("is_monotonic")? {
            i.extract::<bool>()?
        } else {
            false
        };
        Ok(otlp_mmap_protocol::metric_ref::Aggregation::Sum(
            otlp_mmap_protocol::metric_ref::Sum {
                aggregation_temporality: temp,
                is_monotonic: mono,
            },
        ))
    } else if let Some(hist_dict) = dict.get_item("histogram")? {
        let d = hist_dict.cast::<PyDict>()?;
        let temp = if let Some(i) = d.get_item("aggregation_temporality")? {
            i.extract::<i32>()?
        } else {
            0
        };
        let bounds = if let Some(i) = d.get_item("bucket_boundaries")? {
            i.extract::<Vec<f64>>()?
        } else {
            Vec::new()
        };
        Ok(otlp_mmap_protocol::metric_ref::Aggregation::Histogram(
            otlp_mmap_protocol::metric_ref::Histogram {
                aggregation_temporality: temp,
                bucket_boundares: bounds,
            },
        ))
    } else if let Some(exp_dict) = dict.get_item("exp_histogram")? {
        let d = exp_dict.downcast::<PyDict>()?;
        let temp = if let Some(i) = d.get_item("aggregation_temporality")? {
            i.extract::<i32>()?
        } else {
            0
        };
        let buckets = if let Some(i) = d.get_item("max_buckets")? {
            i.extract::<i64>()?
        } else {
            0
        };
        let scale = if let Some(i) = d.get_item("max_scale")? {
            i.extract::<i64>()?
        } else {
            0
        };
        Ok(otlp_mmap_protocol::metric_ref::Aggregation::ExpHist(
            otlp_mmap_protocol::metric_ref::ExponentialHistogram {
                aggregation_temporality: temp,
                max_buckets: buckets,
                max_scale: scale,
            },
        ))
    } else {
        Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Unknown aggregation type",
        ))
    }
}

fn spin_lock_write<T: prost::Message + std::fmt::Debug>(
    py: Python<'_>,
    ring: &RingBufferWriter<T>,
    msg: &T,
) -> PyResult<()> {
    // Fast spin
    for _ in 0..10 {
        if ring.try_write(msg).map_err(core_to_py_err)? {
            return Ok(());
        } else {
            std::hint::spin_loop();
        }
    }
    // If we fail, we drop the GIL and enter a more aggressive yield
    py.detach(|| {
        for _ in 0..100 {
            if ring.try_write(msg).map_err(core_to_py_err)? {
                return Ok(());
            } else {
                std::thread::yield_now();
            }
        }
        // Sleep spin, exponentially slower.
        // TODO - We probably don't need or *want* this in the hot path, we should just force-write the message as our
        // reader may be dead.
        // We copy this over just for solidarity with the mmap-collector side.
        let mut d = Duration::from_millis(1);
        loop {
            if ring.try_write(msg).map_err(core_to_py_err)? {
                // println!("Read {} event on slow path", std::any::type_name::<T>());
                return Ok(());
            } else {
                std::thread::sleep(d);
            }
            if d.as_secs() < 1 {
                d *= 2;
            }
        }
    })
}
