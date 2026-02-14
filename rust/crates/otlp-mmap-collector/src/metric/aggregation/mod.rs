//! Aggregation extraction for metric SDK implementation.

mod exp_hist;
mod gauge;
mod no_aggregation;
mod sum;

use gauge::GaugeAggregationConfig;
use no_aggregation::NoAggregationConfig;
use otlp_mmap_protocol::Measurement;
use sum::SumConfig;

use crate::{
    metric::{timeseries_id::TimeSeriesIdentity, CollectionContext},
    Error,
};

/// Converts from an SDK mmap metric configuration to an aggregation.
pub fn convert_sdk_mmap_config(
    config: Option<otlp_mmap_protocol::metric_ref::Aggregation>,
) -> Box<dyn AggregationConfig> {
    match config {
        Some(otlp_mmap_protocol::metric_ref::Aggregation::Gauge(_)) => {
            Box::new(GaugeAggregationConfig {})
        }
        Some(otlp_mmap_protocol::metric_ref::Aggregation::Sum(sum)) => Box::new(SumConfig {
            is_monotonic: sum.is_monotonic,
            aggregation_temporality: sum.aggregation_temporality,
        }),
        Some(otlp_mmap_protocol::metric_ref::Aggregation::Histogram(_hist)) => {
            // TODO - Actually do regular histograms.
            Box::new(exp_hist::BucketConfig {
                max_size: 100,
                max_scale: 20,
            })
        }
        Some(otlp_mmap_protocol::metric_ref::Aggregation::ExpHist(ehist)) => {
            Box::new(exp_hist::BucketConfig {
                max_size: ehist.max_buckets as i32,
                max_scale: ehist.max_scale as i8,
            })
        }
        _ => Box::new(NoAggregationConfig {}),
    }
}

/// An implementation of aggregation views for metrics.
///
/// This needs to be able to:
/// - Allocate new storage for newly discovered timeseries.
/// - Allocate new storage on collection, for recording current
///   aggregated values.
pub trait AggregationConfig {
    fn new_aggregation(&self) -> Box<dyn Aggregation>;

    /// Constructs a new data we can use to fill out timeseries.
    /// Returning none, means this aggregation does not return values.
    fn new_collection_data(&self) -> Option<opentelemetry_proto::tonic::metrics::v1::metric::Data>;
}

/// This is the storage which actually performs aggregation for
/// metrics.
pub trait Aggregation {
    /// Joins the found metric into the current aggregation.
    fn join(&mut self, m: Measurement) -> Result<(), Error>;

    /// Collects the current value into the given OTLP structure.
    fn collect(
        &self,
        id: &TimeSeriesIdentity,
        ctx: &CollectionContext,
        cell: &mut opentelemetry_proto::tonic::metrics::v1::metric::Data,
    );
}
