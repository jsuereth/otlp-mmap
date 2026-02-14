//! Sum Aggregation

/// Configuration for a SUM.
pub struct SumConfig {
    /// Whether we allow negative measurements.
    pub is_monotonic: bool,
    /// CUMULATIVE or DELTA.
    pub aggregation_temporality: i32,
}
impl super::AggregationConfig for SumConfig {
    fn new_aggregation(&self) -> Box<dyn super::Aggregation> {
        Box::new(SumAggregation { latest_sum: 0. })
    }

    fn new_collection_data(&self) -> Option<opentelemetry_proto::tonic::metrics::v1::metric::Data> {
        Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(
            opentelemetry_proto::tonic::metrics::v1::Sum {
                data_points: Vec::new(),
                aggregation_temporality: self.aggregation_temporality,
                is_monotonic: self.is_monotonic,
            },
        ))
    }
}

struct SumAggregation {
    latest_sum: f64,
    // TODO - exemplars
    // TODO - monotonic changes.
}
impl super::Aggregation for SumAggregation {
    fn join(&mut self, m: otlp_mmap_protocol::Measurement) -> Result<(), crate::Error> {
        // TODO - exemplars, timestamps, etc.
        if let Some(v) = m.value {
            match v {
                otlp_mmap_protocol::measurement::Value::AsLong(lv) => self.latest_sum += lv as f64,
                otlp_mmap_protocol::measurement::Value::AsDouble(dv) => self.latest_sum += dv,
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
        if let opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(sum) = cell {
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
                        self.latest_sum,
                    ),
                ),
            };
            sum.data_points.push(point);
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
    fn test_sum_aggregation_long() {
        let config = SumConfig {
            is_monotonic: true,
            aggregation_temporality: 1, // Delta
        };
        let mut agg = config.new_aggregation();
        let id = TimeSeriesIdentity::new(vec![]);
        let ctx = CollectionContext::new(100, 200);
        let mut data = config.new_collection_data().unwrap();

        agg.join(Measurement {
            metric_ref: 1,
            attributes: vec![],
            time_unix_nano: 150,
            span_context: None,
            value: Some(Value::AsLong(10)),
        })
        .unwrap();

        agg.join(Measurement {
            metric_ref: 1,
            attributes: vec![],
            time_unix_nano: 160,
            span_context: None,
            value: Some(Value::AsLong(20)),
        })
        .unwrap();

        agg.collect(&id, &ctx, &mut data);

        if let opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(sum) = data {
            assert_eq!(sum.data_points.len(), 1);
            let dp = &sum.data_points[0];
            assert_eq!(dp.start_time_unix_nano, 100);
            assert_eq!(dp.time_unix_nano, 200);
            if let Some(
                opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(v),
            ) = dp.value
            {
                assert_eq!(v, 30.0);
            } else {
                panic!("Expected double value");
            }
        } else {
            panic!("Expected Sum data");
        }
    }

    #[test]
    fn test_sum_aggregation_double() {
        let config = SumConfig {
            is_monotonic: true,
            aggregation_temporality: 1,
        };
        let mut agg = config.new_aggregation();
        let id = TimeSeriesIdentity::new(vec![]);
        let ctx = CollectionContext::new(100, 200);
        let mut data = config.new_collection_data().unwrap();

        agg.join(Measurement {
            metric_ref: 1,
            attributes: vec![],
            time_unix_nano: 150,
            span_context: None,
            value: Some(Value::AsDouble(10.5)),
        })
        .unwrap();

        agg.join(Measurement {
            metric_ref: 1,
            attributes: vec![],
            time_unix_nano: 160,
            span_context: None,
            value: Some(Value::AsDouble(20.25)),
        })
        .unwrap();

        agg.collect(&id, &ctx, &mut data);

        if let opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(sum) = data {
            assert_eq!(sum.data_points.len(), 1);
            let dp = &sum.data_points[0];
            if let Some(
                opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(v),
            ) = dp.value
            {
                assert_eq!(v, 30.75);
            } else {
                panic!("Expected double value");
            }
        } else {
            panic!("Expected Sum data");
        }
    }

    #[test]
    fn test_sum_aggregation_mixed() {
        let config = SumConfig {
            is_monotonic: true,
            aggregation_temporality: 1,
        };
        let mut agg = config.new_aggregation();
        let id = TimeSeriesIdentity::new(vec![]);
        let ctx = CollectionContext::new(100, 200);
        let mut data = config.new_collection_data().unwrap();

        agg.join(Measurement {
            metric_ref: 1,
            attributes: vec![],
            time_unix_nano: 150,
            span_context: None,
            value: Some(Value::AsLong(10)),
        })
        .unwrap();

        agg.join(Measurement {
            metric_ref: 1,
            attributes: vec![],
            time_unix_nano: 160,
            span_context: None,
            value: Some(Value::AsDouble(20.5)),
        })
        .unwrap();

        agg.collect(&id, &ctx, &mut data);

        if let opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(sum) = data {
            assert_eq!(sum.data_points.len(), 1);
            let dp = &sum.data_points[0];
            if let Some(
                opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(v),
            ) = dp.value
            {
                assert_eq!(v, 30.5);
            } else {
                panic!("Expected double value");
            }
        } else {
            panic!("Expected Sum data");
        }
    }
}
