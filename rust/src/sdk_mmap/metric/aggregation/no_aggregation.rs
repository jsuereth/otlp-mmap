//! Aggregation which drops data on the floor.

use crate::{
    oltp_mmap::Error,
    sdk_mmap::{
        data::Measurement,
        metric::{CollectionContext, TimeSeriesIdentity},
    },
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
    fn join(&mut self, m: Measurement) -> Result<(), Error> {
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
