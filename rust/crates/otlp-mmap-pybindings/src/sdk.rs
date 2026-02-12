//! Implementation of key SDK features for OTLP-MMAP, including high-performance, concurrent hashing.

use std::path::Path;
use std::time::Duration;

use otlp_mmap_core::{OtlpMmapWriter, RingBufferWriter};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use scc::HashIndex;

use crate::core_to_py_err;

/// Helper for writing values to OTLP-MMAP that will attempt to re-use/cache dictionary items across
/// various SDK calls.
pub(crate) struct SdkWriter {
    /// Writer of values to the OTLP-MMAP file.
    writer: OtlpMmapWriter,
    /// Cache of previously written keys in the dictionary.
    key_cache: HashIndex<String, i64>,
    // TODO - Resoure cache?
    // TODO - InstrumentationScope cache?
    // TODO - Metric cache?
}

impl SdkWriter {
    /// Constructs a new SdkWriter.
    pub fn new(path: &Path, config: &otlp_mmap_core::OtlpMmapConfig) -> PyResult<Self> {
        Ok(Self {
            writer: OtlpMmapWriter::new(Path::new(path), &config).map_err(core_to_py_err)?,
            key_cache: HashIndex::new(),
        })
    }

    /// Helper to record a span event into the ring buffer.
    pub fn record_span_event(
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
    pub fn record_measurement(
        &self,
        py: Python<'_>,
        measurement: otlp_mmap_protocol::Measurement,
    ) -> PyResult<()> {
        spin_lock_write(py, self.writer.measurements(), &measurement)
    }

    /// spin-lock write of events to our ring buffer.
    pub fn record_event(&self, py: Python<'_>, event: otlp_mmap_protocol::Event) -> PyResult<()> {
        spin_lock_write(py, self.writer.events(), &event)
    }

    /// Records the string in the dictionary or returns cached pervious recording.
    pub fn intern_string(&self, value: &str) -> PyResult<i64> {
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

    /// Records the resource in the dictionary or returns cached pervious recording.
    pub fn intern_resource(
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

    /// Records the resource in the dictionary or returns cached pervious recording.
    pub fn intern_instrumentation_scope(
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

    /// Records the metric definition in the dictionary or returns cached pervious recording.
    pub fn intern_metric_stream(
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
    pub fn convert_attributes(
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
