//! Gauge Aggregation

use crate::sdk_mmap::Error;

/// Configuration for a Gauge aggregation.
pub struct GaugeAggregationConfig {}
impl super::AggregationConfig for GaugeAggregationConfig {
    fn new_aggregation(&self) -> Box<dyn super::Aggregation> {
        Box::new(GaugeAggregation {
            latest_measurement: 0.,
        })
    }

    fn new_collection_data(&self) -> Option<opentelemetry_proto::tonic::metrics::v1::metric::Data> {
        Some(
            opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(
                opentelemetry_proto::tonic::metrics::v1::Gauge {
                    data_points: Vec::new(),
                },
            ),
        )
    }
}

/// "cell" of aggregation for a Gauge.
struct GaugeAggregation {
    latest_measurement: f64, // TODO - exemplars
}
impl super::Aggregation for GaugeAggregation {
    fn join(&mut self, m: super::Measurement) -> Result<(), Error> {
        // TODO - exemplars, timestamps, etc.
        if let Some(v) = m.value {
            match v {
                super::super::data::measurement::Value::AsLong(lv) => {
                    self.latest_measurement = lv as f64
                }
                super::super::data::measurement::Value::AsDouble(dv) => {
                    self.latest_measurement = dv
                }
            }
        }
        Ok(())
    }

    fn collect(
        &self,
        id: &super::TimeSeriesIdentity,
        ctx: &super::CollectionContext,
        cell: &mut opentelemetry_proto::tonic::metrics::v1::metric::Data,
    ) {
        if let opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(gauge) = cell {
            let point = opentelemetry_proto::tonic::metrics::v1::NumberDataPoint {
                attributes: id.to_otlp_attributes(),
                start_time_unix_nano: ctx.start_unix_nano,
                time_unix_nano: ctx.current_unix_nano,
                exemplars: Vec::new(),
                // We don't allow flags
                flags: 0,
                // TODO - support int or double.
                value: Some(
                    opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(
                        self.latest_measurement,
                    ),
                ),
            };
            gauge.data_points.push(point);
        }
    }
}
