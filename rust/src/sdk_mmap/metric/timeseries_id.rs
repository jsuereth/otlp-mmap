//! Timeseries identity helpers.

use crate::{
    oltp_mmap::Error,
    sdk_mmap::{data::KeyValueRef, CollectorSdk},
};

/// A hashable time series identity.
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct TimeSeriesIdentity {}
impl TimeSeriesIdentity {
    pub async fn new(
        attributes: &[KeyValueRef],
        sdk: &CollectorSdk,
    ) -> Result<TimeSeriesIdentity, Error> {
        todo!()
    }

    pub fn to_otlp_attributes(&self) -> Vec<opentelemetry_proto::tonic::common::v1::KeyValue> {
        todo!()
    }
}
