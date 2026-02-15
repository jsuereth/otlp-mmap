//! Metric SDK implementation

use std::collections::{btree_map::Entry, BTreeMap};

use otlp_mmap_protocol::Measurement;

use crate::{
    metric::{
        aggregation::{Aggregation, AggregationConfig},
        timeseries_id::TimeSeriesIdentity,
    },
    Error, SdkLookup,
};

mod aggregation;
mod timeseries_id;

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
    pub fn handle_measurement(
        &mut self,
        lookup: &impl SdkLookup,
        measurement: Measurement,
    ) -> Result<(), Error> {
        match self.metrics.entry(measurement.metric_ref) {
            Entry::Vacant(entry) => entry
                .insert(MetricAggregator::new(measurement.metric_ref, lookup)?)
                .handle(lookup, measurement),
            Entry::Occupied(mut aggregator) => aggregator.get_mut().handle(lookup, measurement),
        }
    }

    /// Collects the metrics in this storage.
    /// TODO - add "end" timestamp.
    pub fn collect(&self, ctx: &CollectionContext) -> Vec<CollectedMetric> {
        self.metrics
            .values()
            .filter_map(|storage| {
                storage.collect(ctx).map(|metric| CollectedMetric {
                    scope_ref: storage.scope_ref,
                    metric,
                })
            })
            .collect()
    }
}

struct MetricAggregator {
    name: String,
    unit: String,
    description: String,
    /// The aggregation configuration, as a thing we can use to build storage.
    aggregation: Box<dyn AggregationConfig>,
    /// The active timeseries in this current metric.
    timeseries: BTreeMap<TimeSeriesIdentity, Box<dyn Aggregation>>,
    /// Reference to an instrumentation scope.
    scope_ref: i64,
}

impl MetricAggregator {
    /// Constructs a new metric aggregator.
    fn new(metric_ref: i64, dictionary: &impl SdkLookup) -> Result<MetricAggregator, Error> {
        let definition = dictionary.try_lookup_metric(metric_ref)?;
        println!(
            "Discovered metric <{} on scope:{}>",
            definition.name, definition.instrumentation_scope_ref
        );
        // TODO - read exemplar config?
        let aggregation: Box<dyn AggregationConfig> =
            aggregation::convert_sdk_mmap_config(definition.aggregation);
        Ok(MetricAggregator {
            name: definition.name,
            unit: definition.unit,
            description: definition.description,
            timeseries: BTreeMap::new(),
            aggregation,
            scope_ref: definition.instrumentation_scope_ref,
        })
    }

    /// Takes a measurement and passes it into the appropriate aggregation.
    fn handle(&mut self, lookup: &impl SdkLookup, measurement: Measurement) -> Result<(), Error> {
        // TODO - do we need to convert name_ref into name to deal with possible duplicates in dictionary?
        // TODO - figure out which attributes are NOT kept in timeseries for this.
        let id = TimeSeriesIdentity::from_keyvalue_refs(&measurement.attributes, lookup)?;
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
