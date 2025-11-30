//! Timeseries identity helpers.

use crate::{
    sdk_mmap::Error,
    sdk_mmap::{data::KeyValueRef, CollectorSdk},
};

/// A hashable time series identity.
pub struct TimeSeriesIdentity {
    attributes: Vec<opentelemetry_proto::tonic::common::v1::KeyValue>,
}
impl TimeSeriesIdentity {
    /// Constructs a new timeseries identifier from the given attribute key value refs.
    pub async fn new(
        attributes: &[KeyValueRef],
        sdk: &CollectorSdk,
    ) -> Result<TimeSeriesIdentity, Error> {
        let mut kvs = Vec::new();
        for kv in attributes {
            // TODO - avoid copying kv here.
            kvs.push(sdk.try_convert_attribute(kv.to_owned()).await?);
        }
        // Sort by key name for faster comparisons later.
        kvs.sort_by(|l, r| l.key.cmp(&r.key));
        Ok(TimeSeriesIdentity { attributes: kvs })
    }

    pub fn to_otlp_attributes(&self) -> Vec<opentelemetry_proto::tonic::common::v1::KeyValue> {
        self.attributes.clone()
    }
}

impl PartialEq for TimeSeriesIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.attributes == other.attributes
    }
}
impl Eq for TimeSeriesIdentity {}

impl PartialOrd for TimeSeriesIdentity {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimeSeriesIdentity {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // TODO - Check Efficiency of implementation.
        for (l, r) in self.attributes.iter().zip(other.attributes.iter()) {
            match l.key.cmp(&r.key) {
                std::cmp::Ordering::Less => return std::cmp::Ordering::Less,
                std::cmp::Ordering::Greater => return std::cmp::Ordering::Greater,
                std::cmp::Ordering::Equal => match compare_opt_values(&l.value, &r.value) {
                    std::cmp::Ordering::Less => return std::cmp::Ordering::Less,
                    std::cmp::Ordering::Greater => return std::cmp::Ordering::Greater,
                    std::cmp::Ordering::Equal => (),
                },
            }
        }
        std::cmp::Ordering::Equal
    }
}

fn compare_opt_values(
    l: &Option<opentelemetry_proto::tonic::common::v1::AnyValue>,
    r: &Option<opentelemetry_proto::tonic::common::v1::AnyValue>,
) -> std::cmp::Ordering {
    match (
        l.as_ref().and_then(|v| v.value.as_ref()),
        r.as_ref().and_then(|v| v.value.as_ref()),
    ) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(lv), Some(rv)) => compare_values(lv, rv),
    }
}

fn compare_values(
    l: &opentelemetry_proto::tonic::common::v1::any_value::Value,
    r: &opentelemetry_proto::tonic::common::v1::any_value::Value,
) -> std::cmp::Ordering {
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    // TODO - We need to handle same key id, but different types...  Treat them the same if their "to_string" is the same.
    match (l, r) {
        (Value::StringValue(ls), Value::StringValue(rs)) => ls.cmp(rs),
        (Value::StringValue(_), Value::BoolValue(_)) => todo!(),
        (Value::StringValue(_), Value::IntValue(_)) => todo!(),
        (Value::StringValue(_), Value::DoubleValue(_)) => todo!(),
        (Value::StringValue(_), Value::ArrayValue(_)) => todo!(),
        (Value::StringValue(_), Value::KvlistValue(_)) => todo!(),
        (Value::StringValue(_), Value::BytesValue(_)) => todo!(),
        (Value::BoolValue(_), Value::StringValue(_)) => todo!(),
        (Value::BoolValue(lv), Value::BoolValue(rv)) => lv.cmp(rv),
        (Value::BoolValue(_), Value::IntValue(_)) => todo!(),
        (Value::BoolValue(_), Value::DoubleValue(_)) => todo!(),
        (Value::BoolValue(_), Value::ArrayValue(_)) => todo!(),
        (Value::BoolValue(_), Value::KvlistValue(_)) => todo!(),
        (Value::BoolValue(_), Value::BytesValue(_)) => todo!(),
        (Value::IntValue(_), Value::StringValue(_)) => todo!(),
        (Value::IntValue(_), Value::BoolValue(_)) => todo!(),
        (Value::IntValue(lv), Value::IntValue(rv)) => lv.cmp(rv),
        (Value::IntValue(_), Value::DoubleValue(_)) => todo!(),
        (Value::IntValue(_), Value::ArrayValue(_)) => todo!(),
        (Value::IntValue(_), Value::KvlistValue(_)) => todo!(),
        (Value::IntValue(_), Value::BytesValue(_)) => todo!(),
        (Value::DoubleValue(_), Value::StringValue(_)) => todo!(),
        (Value::DoubleValue(_), Value::BoolValue(_)) => todo!(),
        (Value::DoubleValue(_), Value::IntValue(_)) => todo!(),
        (Value::DoubleValue(lv), Value::DoubleValue(rv)) => lv.total_cmp(rv),
        (Value::DoubleValue(_), Value::ArrayValue(_)) => todo!(),
        (Value::DoubleValue(_), Value::KvlistValue(_)) => todo!(),
        (Value::DoubleValue(_), Value::BytesValue(_)) => todo!(),
        (Value::ArrayValue(_), Value::StringValue(_)) => todo!(),
        (Value::ArrayValue(_), Value::BoolValue(_)) => todo!(),
        (Value::ArrayValue(_), Value::IntValue(_)) => todo!(),
        (Value::ArrayValue(_), Value::DoubleValue(_)) => todo!(),
        (Value::ArrayValue(_), Value::ArrayValue(_)) => todo!(),
        (Value::ArrayValue(_), Value::KvlistValue(_)) => todo!(),
        (Value::ArrayValue(_), Value::BytesValue(_)) => todo!(),
        (Value::KvlistValue(_), Value::StringValue(_)) => todo!(),
        (Value::KvlistValue(_), Value::BoolValue(_)) => todo!(),
        (Value::KvlistValue(_), Value::IntValue(_)) => todo!(),
        (Value::KvlistValue(_), Value::DoubleValue(_)) => todo!(),
        (Value::KvlistValue(_), Value::ArrayValue(_)) => todo!(),
        (Value::KvlistValue(_), Value::KvlistValue(_)) => todo!(),
        (Value::KvlistValue(_), Value::BytesValue(_)) => todo!(),
        (Value::BytesValue(_), Value::StringValue(_)) => todo!(),
        (Value::BytesValue(_), Value::BoolValue(_)) => todo!(),
        (Value::BytesValue(_), Value::IntValue(_)) => todo!(),
        (Value::BytesValue(_), Value::DoubleValue(_)) => todo!(),
        (Value::BytesValue(_), Value::ArrayValue(_)) => todo!(),
        (Value::BytesValue(_), Value::KvlistValue(_)) => todo!(),
        (Value::BytesValue(_), Value::BytesValue(_)) => todo!(),
    }
}
