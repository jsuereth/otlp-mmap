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
    fn join(
        &mut self,
        m: crate::sdk_mmap::data::Measurement,
    ) -> Result<(), crate::oltp_mmap::Error> {
        // TODO - exemplars, timestamps, etc.
        if let Some(v) = m.value {
            match v {
                super::super::data::measurement::Value::AsLong(lv) => self.latest_sum += lv as f64,
                super::super::data::measurement::Value::AsDouble(dv) => self.latest_sum += dv,
            }
        }
        Ok(())
    }

    fn collect(
        &self,
        id: &crate::sdk_mmap::metric::timeseries_id::TimeSeriesIdentity,
        ctx: &crate::sdk_mmap::metric::CollectionContext,
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
