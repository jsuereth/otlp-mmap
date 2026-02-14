//! Aggregation which drops data on the floor.

use otlp_mmap_protocol::Measurement;

use crate::{
    metric::{timeseries_id::TimeSeriesIdentity, CollectionContext},
    Error,
};

/// Aggregation which does not remember any metric.
pub struct NoAggregationConfig {}
impl super::AggregationConfig for NoAggregationConfig {
    fn new_aggregation(&self) -> Box<dyn super::Aggregation> {
        // TODO - don't allocate any new memory.
        Box::new(NoAggregation {})
    }
    fn new_collection_data(&self) -> Option<opentelemetry_proto::tonic::metrics::v1::metric::Data> {
        None
    }
}

/// Aggregation cell which stores nothing.
pub struct NoAggregation {}
// Aggregation which does nothing.
impl super::Aggregation for NoAggregation {
    fn join(&mut self, _m: Measurement) -> Result<(), Error> {
        Ok(())
    }

    fn collect(
        &self,
        _: &TimeSeriesIdentity,
        _: &CollectionContext,
        _: &mut opentelemetry_proto::tonic::metrics::v1::metric::Data,
    ) {
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metric::aggregation::{Aggregation, AggregationConfig};
    use otlp_mmap_protocol::measurement::Value;
    use otlp_mmap_protocol::Measurement;

    #[test]
    fn test_no_aggregation() {
        let config = NoAggregationConfig {};
        let mut agg = config.new_aggregation();
        let data = config.new_collection_data();

        assert!(data.is_none());

        agg.join(Measurement {
            metric_ref: 1,
            attributes: vec![],
            time_unix_nano: 150,
            span_context: None,
            value: Some(Value::AsLong(10)),
        })
        .unwrap();

        // collect is a no-op, but we can't really call it without valid Data,
        // which new_collection_data doesn't provide.
    }
}
