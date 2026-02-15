//! Gauge Aggregation

/// Configuration for a Gauge.
pub struct GaugeAggregationConfig {}
impl super::AggregationConfig for GaugeAggregationConfig {
    fn new_aggregation(&self) -> Box<dyn super::Aggregation> {
        Box::new(GaugeAggregation { latest_value: 0. })
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

struct GaugeAggregation {
    latest_value: f64,
}
impl super::Aggregation for GaugeAggregation {
    fn join(&mut self, m: otlp_mmap_protocol::Measurement) -> Result<(), crate::Error> {
        if let Some(v) = m.value {
            match v {
                otlp_mmap_protocol::measurement::Value::AsLong(lv) => self.latest_value = lv as f64,
                otlp_mmap_protocol::measurement::Value::AsDouble(dv) => self.latest_value = dv,
            }
        }
        Ok(())
    }

    fn collect(
        &self,
        id: &crate::metric::timeseries_id::TimeSeriesIdentity,
        ctx: &crate::metric::CollectionContext,
        cell: &mut opentelemetry_proto::tonic::metrics::v1::metric::Data,
    ) {
        if let opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(gauge) = cell {
            let point = opentelemetry_proto::tonic::metrics::v1::NumberDataPoint {
                attributes: id.to_otlp_attributes(),
                start_time_unix_nano: ctx.start_unix_nano,
                time_unix_nano: ctx.current_unix_nano,
                exemplars: Vec::new(),
                flags: 0,
                value: Some(
                    opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(
                        self.latest_value,
                    ),
                ),
            };
            gauge.data_points.push(point);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metric::aggregation::{Aggregation, AggregationConfig};
    use crate::metric::timeseries_id::TimeSeriesIdentity;
    use crate::metric::CollectionContext;
    use otlp_mmap_protocol::measurement::Value;
    use otlp_mmap_protocol::Measurement;

    #[test]
    fn test_gauge_aggregation_latest() {
        let config = GaugeAggregationConfig {};
        let mut agg = config.new_aggregation();
        let id = TimeSeriesIdentity::new(vec![]);
        let ctx = CollectionContext::new(100, 200);
        let mut data = config
            .new_collection_data()
            .expect("Failed to create collection data");

        agg.join(Measurement {
            metric_ref: 1,
            attributes: vec![],
            time_unix_nano: 150,
            span_context: None,
            value: Some(Value::AsLong(10)),
        })
        .expect("Failed to join measurement");

        agg.join(Measurement {
            metric_ref: 1,
            attributes: vec![],
            time_unix_nano: 160,
            span_context: None,
            value: Some(Value::AsLong(20)),
        })
        .expect("Failed to join measurement");

        agg.collect(&id, &ctx, &mut data);

        if let opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(gauge) = data {
            assert_eq!(gauge.data_points.len(), 1);
            let dp = &gauge.data_points[0];
            assert_eq!(dp.start_time_unix_nano, 100);
            assert_eq!(dp.time_unix_nano, 200);
            if let Some(
                opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(v),
            ) = dp.value
            {
                assert_eq!(v, 20.0);
            } else {
                panic!("Expected double value");
            }
        } else {
            panic!("Expected Gauge data");
        }
    }

    #[test]
    fn test_gauge_aggregation_double() {
        let config = GaugeAggregationConfig {};
        let mut agg = config.new_aggregation();
        let id = TimeSeriesIdentity::new(vec![]);
        let ctx = CollectionContext::new(100, 200);
        let mut data = config
            .new_collection_data()
            .expect("Failed to create collection data");

        agg.join(Measurement {
            metric_ref: 1,
            attributes: vec![],
            time_unix_nano: 150,
            span_context: None,
            value: Some(Value::AsDouble(10.5)),
        })
        .expect("Failed to join measurement");

        agg.join(Measurement {
            metric_ref: 1,
            attributes: vec![],
            time_unix_nano: 160,
            span_context: None,
            value: Some(Value::AsDouble(20.25)),
        })
        .expect("Failed to join measurement");

        agg.collect(&id, &ctx, &mut data);

        if let opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(gauge) = data {
            assert_eq!(gauge.data_points.len(), 1);
            let dp = &gauge.data_points[0];
            if let Some(
                opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(v),
            ) = dp.value
            {
                assert_eq!(v, 20.25);
            } else {
                panic!("Expected double value");
            }
        } else {
            panic!("Expected Gauge data");
        }
    }
}
