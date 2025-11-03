//! Metric SDK implementation

use std::collections::BTreeMap;

use crate::oltp_mmap::Error;
use crate::sdk_mmap::data::{self, KeyValueRef};
use crate::sdk_mmap::{data::Measurement, CollectorSdk};

mod exp_hist;

/// Current value of a collected metric, in OTLP form.
pub struct CollectedMetric {
    /// Reference to the scope in which the metric was collected.
    pub scope_ref: i64,
    /// The metric point value.
    pub metric: opentelemetry_proto::tonic::metrics::v1::Metric,
}

/// Context used when collecting metrics.
pub struct CollectionContext {
    start_unix_nano: u64,
    current_unix_nano: u64,
}
impl CollectionContext {
    pub fn new(start_unix_nano: u64, current_unix_nano: u64) -> CollectionContext {
        CollectionContext {
            start_unix_nano,
            current_unix_nano,
        }
    }
}

/// Metric storage for a single SDK.
pub struct MetricStorage {
    /// Map from metric reference id to the aggregator handling measurements for it.
    metrics: BTreeMap<i64, MetricAggregator>,
}

impl MetricStorage {
    /// Constructs new metric storage.
    pub fn new() -> Self {
        Self {
            metrics: BTreeMap::new(),
        }
    }

    /// Handles an incoming measurement.
    pub async fn handle_measurement(
        &mut self,
        sdk: &CollectorSdk,
        measurement: Measurement,
    ) -> Result<(), Error> {
        let aggregator = self
            .metrics
            .entry(measurement.metric_ref)
            .or_insert(MetricAggregator::new(measurement.metric_ref, sdk).await?);
        // TODO - GC on stale metrics?
        aggregator.handle(sdk, measurement).await
    }

    /// Collects the metrics in this storage.
    /// TODO - add "end" timestamp.
    pub async fn collect(&self, ctx: &CollectionContext) -> Vec<CollectedMetric> {
        self.metrics
            .iter()
            .filter_map(|(m, storage)| {
                storage.collect(ctx).map(|metric| CollectedMetric {
                    scope_ref: *m,
                    metric,
                })
            })
            .collect()
    }
}

struct MetricAggregator {
    // TODO - our metric name/config here.
    scope_ref: i64,
    name: String,
    unit: String,
    description: String,
    /// The aggregation configuration, as a thing we can use to build storage.
    aggregation: Box<dyn AggregationConfig>,
    /// The active timeseries in this current metric.
    timeseries: BTreeMap<TimeSeriesIdentity, Box<dyn Aggregation>>,
}

impl MetricAggregator {
    /// Constructs a new metric aggregator.
    async fn new(metric_ref: i64, sdk: &CollectorSdk) -> Result<MetricAggregator, Error> {
        let definition = sdk.try_lookup_metric(metric_ref).await?;
        // TODO - read exemplar config?
        let aggregation: Box<dyn AggregationConfig> = match definition.aggregation {
            Some(data::metric_ref::Aggregation::Gauge(_)) => Box::new(GaugeAggregationConfig {}),
            Some(data::metric_ref::Aggregation::Sum(sum)) => todo!(),
            Some(data::metric_ref::Aggregation::Histogram(hist)) => todo!(),
            Some(data::metric_ref::Aggregation::ExpHist(ehist)) => todo!(),
            _ => Box::new(NoAggregationConfig {}),
        };
        Ok(MetricAggregator {
            scope_ref: definition.instrumentation_scope_ref,
            name: definition.name,
            unit: definition.unit,
            description: definition.description,
            timeseries: BTreeMap::new(),
            aggregation,
        })
    }

    /// Takes a measurement and passes it into the appropriate aggregation.
    async fn handle(&mut self, sdk: &CollectorSdk, measurement: Measurement) -> Result<(), Error> {
        // TODO - do we need to convert name_ref into name to deal with possible duplicates in dictionary?
        // TODO - figure out which attributes are NOT kept in timeseries for this.
        let id = TimeSeriesIdentity::new(&measurement.attributes, sdk).await?;
        self.timeseries
            .entry(id)
            .or_insert(self.aggregation.new_aggregation())
            .join(measurement)
    }

    fn collect(
        &self,
        ctx: &CollectionContext,
    ) -> Option<opentelemetry_proto::tonic::metrics::v1::Metric> {
        if let Some(mut result) = self.aggregation.new_collection_data() {
            for (id, agg) in &self.timeseries {
                agg.collect(id, ctx, &mut result);
            }
            Some(opentelemetry_proto::tonic::metrics::v1::Metric {
                name: self.name.clone(),
                description: self.description.clone(),
                unit: self.unit.clone(),
                metadata: Vec::new(),
                data: Some(result),
            })
        } else {
            None
        }
    }
}

#[derive(Hash, PartialEq, Eq, PartialOrd, Ord)]
struct TimeSeriesIdentity {}
impl TimeSeriesIdentity {
    async fn new(
        attributes: &[KeyValueRef],
        sdk: &CollectorSdk,
    ) -> Result<TimeSeriesIdentity, Error> {
        todo!()
    }

    fn to_otlp_attributes(&self) -> Vec<opentelemetry_proto::tonic::common::v1::KeyValue> {
        todo!()
    }
}

/// An implementation of aggregation views for metrics.
///
/// This needs to be able to:
/// - Allocate new storage for newly discovered timeseries.
/// - Allocate new storage on collection, for recording current
///   aggregated values.
trait AggregationConfig {
    fn new_aggregation(&self) -> Box<dyn Aggregation>;

    /// Constructs a new data we can use to fill out timeseries.
    /// Returning none, means this aggregation does not return values.
    fn new_collection_data(&self) -> Option<opentelemetry_proto::tonic::metrics::v1::metric::Data>;
}

/// This is the storage which actually performs aggregation for
/// metrics.
trait Aggregation {
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

/// Aggregation which does not remember any metric.
struct NoAggregationConfig {}
impl AggregationConfig for NoAggregationConfig {
    fn new_aggregation(&self) -> Box<dyn Aggregation> {
        // TODO - don't allocate any new memory.
        Box::new(NoAggregation {})
    }
    fn new_collection_data(&self) -> Option<opentelemetry_proto::tonic::metrics::v1::metric::Data> {
        None
    }
}

/// Aggregation cell which stores nothing.
struct NoAggregation {}
// Aggregation which does nothing.
impl Aggregation for NoAggregation {
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

struct GaugeAggregationConfig {}
impl AggregationConfig for GaugeAggregationConfig {
    fn new_aggregation(&self) -> Box<dyn Aggregation> {
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

struct GaugeAggregation {
    latest_measurement: f64, // TODO - exemplars
}
impl Aggregation for GaugeAggregation {
    fn join(&mut self, m: Measurement) -> Result<(), Error> {
        // TODO - exemplars, timestamps, etc.
        if let Some(v) = m.value {
            match v {
                super::data::measurement::Value::AsLong(lv) => self.latest_measurement = lv as f64,
                super::data::measurement::Value::AsDouble(dv) => self.latest_measurement = dv,
            }
        }
        Ok(())
    }

    fn collect(
        &self,
        id: &TimeSeriesIdentity,
        ctx: &CollectionContext,
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
