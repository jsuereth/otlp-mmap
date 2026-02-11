//! Timeseries identity helpers.

use otlp_mmap_protocol::KeyValueRef;

use crate::{AttributeLookup, Error};

/// A hashable time series identity.
#[derive(Debug)]
pub struct TimeSeriesIdentity {
    attributes: Vec<opentelemetry_proto::tonic::common::v1::KeyValue>,
}
impl TimeSeriesIdentity {
    /// Constructs a new timeseries identity.
    ///
    /// Key values MUST be sorted and avoid duplicates.
    ///
    /// For testing.
    #[cfg(test)]
    pub fn new<T: Into<Vec<opentelemetry_proto::tonic::common::v1::KeyValue>>>(
        attributes: T,
    ) -> TimeSeriesIdentity {
        TimeSeriesIdentity {
            attributes: attributes.into(),
        }
    }
    /// Constructs a new timeseries identifier from the given attribute key value refs.
    pub fn from_keyvalue_refs<T: AttributeLookup>(
        attributes: &[KeyValueRef],
        sdk: &T,
    ) -> Result<TimeSeriesIdentity, Error> {
        let mut kvs = Vec::new();
        for kv in attributes {
            // TODO - avoid copying kv here.
            kvs.push(sdk.try_convert_attribute(kv.clone())?);
        }
        // Sort by key name for faster comparisons later.
        kvs.sort_by(|l, r| l.key.cmp(&r.key));
        // TODO - remove duplicate keys.
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

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{
        any_value::Value as OTLPValue, AnyValue as OTLPAnyValue, KeyValue,
    };
    use otlp_mmap_protocol::{any_value::Value, AnyValue};
    fn kv(key: &str, value: OTLPValue) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(OTLPAnyValue { value: Some(value) }),
        }
    }

    // Mock implementation of `AttributeLookup` for testing `from_keyvalue_refs`
    struct MockAttributeLookup;

    impl AttributeLookup for MockAttributeLookup {
        fn try_convert_attribute(
            &self,
            kv_ref: KeyValueRef,
        ) -> Result<opentelemetry_proto::tonic::common::v1::KeyValue, Error> {
            let key_string = format!("key_{}", kv_ref.key_ref); // Simplified key lookup

            let otlp_value = if let Some(any_value) = kv_ref.value {
                any_value.value.map(|v| {
                    let new_val = match v {
                        Value::StringValue(s) => OTLPValue::StringValue(s),
                        Value::BoolValue(b) => OTLPValue::BoolValue(b),
                        Value::IntValue(i) => OTLPValue::IntValue(i),
                        Value::DoubleValue(d) => OTLPValue::DoubleValue(d),
                        Value::BytesValue(b) => OTLPValue::BytesValue(b),
                        _ => todo!(), // For ArrayValue, KvlistValue, ValueRef
                    };
                    OTLPAnyValue {
                        value: Some(new_val),
                    }
                })
            } else {
                None
            };

            Ok(opentelemetry_proto::tonic::common::v1::KeyValue {
                key: key_string,
                value: otlp_value,
            })
        }
    }

    #[test]
    fn test_new_timeseries_identity() {
        let attributes = vec![kv("key1", OTLPValue::StringValue("value1".to_string()))];
        let id = TimeSeriesIdentity::new(attributes.clone());
        assert_eq!(id.attributes, attributes);
    }

    #[test]
    fn test_to_otlp_attributes() {
        let attributes = vec![kv("key1", OTLPValue::StringValue("value1".to_string()))];
        let id = TimeSeriesIdentity::new(attributes.clone());
        assert_eq!(id.to_otlp_attributes(), attributes);
    }

    #[test]
    fn test_partial_eq_timeseries_identity() {
        let id1 = TimeSeriesIdentity::new(vec![kv(
            "key1",
            OTLPValue::StringValue("value1".to_string()),
        )]);
        let id2 = TimeSeriesIdentity::new(vec![kv(
            "key1",
            OTLPValue::StringValue("value1".to_string()),
        )]);
        let id3 = TimeSeriesIdentity::new(vec![kv(
            "key2",
            OTLPValue::StringValue("value2".to_string()),
        )]);

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_from_keyvalue_refs_sorts_keys() {
        let sdk = MockAttributeLookup;
        let attributes_unsorted = vec![
            KeyValueRef {
                key_ref: 2,
                value: Some(AnyValue {
                    value: Some(Value::StringValue("valueB".to_string())),
                }),
            },
            KeyValueRef {
                key_ref: 1,
                value: Some(AnyValue {
                    value: Some(Value::StringValue("valueA".to_string())),
                }),
            },
        ];
        let id = TimeSeriesIdentity::from_keyvalue_refs(&attributes_unsorted, &sdk).unwrap();

        assert_eq!(id.attributes[0].key, "key_1");
        assert_eq!(id.attributes[1].key, "key_2");
    }

    #[test]
    fn test_ord_timeseries_identity_string_values() {
        let id1 =
            TimeSeriesIdentity::new(vec![kv("key", OTLPValue::StringValue("apple".to_string()))]);
        let id2 = TimeSeriesIdentity::new(vec![kv(
            "key",
            OTLPValue::StringValue("banana".to_string()),
        )]);
        let id3 =
            TimeSeriesIdentity::new(vec![kv("key", OTLPValue::StringValue("apple".to_string()))]);

        assert!(id1 < id2);
        assert!(id2 > id1);
        assert!(id1 <= id2);
        assert!(id2 >= id1);
        assert_eq!(id1, id3);
        assert!(id1 <= id3);
        assert!(id1 >= id3);
    }

    #[test]
    fn test_ord_timeseries_identity_int_values() {
        let id1 = TimeSeriesIdentity::new(vec![kv("key", OTLPValue::IntValue(1))]);
        let id2 = TimeSeriesIdentity::new(vec![kv("key", OTLPValue::IntValue(2))]);
        let id3 = TimeSeriesIdentity::new(vec![kv("key", OTLPValue::IntValue(1))]);

        assert!(id1 < id2);
        assert!(id2 > id1);
        assert!(id1 <= id2);
        assert!(id2 >= id1);
        assert_eq!(id1, id3);
        assert!(id1 <= id3);
        assert!(id1 >= id3);
    }

    #[test]
    fn test_ord_timeseries_identity_bool_values() {
        let id1 = TimeSeriesIdentity::new(vec![kv("key", OTLPValue::BoolValue(false))]);
        let id2 = TimeSeriesIdentity::new(vec![kv("key", OTLPValue::BoolValue(true))]);
        let id3 = TimeSeriesIdentity::new(vec![kv("key", OTLPValue::BoolValue(false))]);

        assert!(id1 < id2);
        assert!(id2 > id1);
        assert!(id1 <= id2);
        assert!(id2 >= id1);
        assert_eq!(id1, id3);
        assert!(id1 <= id3);
        assert!(id1 >= id3);
    }

    #[test]
    fn test_ord_timeseries_identity_double_values() {
        let id1 = TimeSeriesIdentity::new(vec![kv("key", OTLPValue::DoubleValue(1.0))]);
        let id2 = TimeSeriesIdentity::new(vec![kv("key", OTLPValue::DoubleValue(2.0))]);
        let id3 = TimeSeriesIdentity::new(vec![kv("key", OTLPValue::DoubleValue(1.0))]);

        assert!(id1 < id2);
        assert!(id2 > id1);
        assert!(id1 <= id2);
        assert!(id2 >= id1);
        assert_eq!(id1, id3);
        assert!(id1 <= id3);
        assert!(id1 >= id3);
    }

    #[test]
    fn test_ord_timeseries_identity_multiple_attributes() {
        let id1 = TimeSeriesIdentity::new(vec![
            kv("key1", OTLPValue::StringValue("value1".to_string())),
            kv("key2", OTLPValue::IntValue(1)),
        ]);
        let id2 = TimeSeriesIdentity::new(vec![
            kv("key1", OTLPValue::StringValue("value1".to_string())),
            kv("key2", OTLPValue::IntValue(2)),
        ]);
        let id3 = TimeSeriesIdentity::new(vec![
            kv("key1", OTLPValue::StringValue("value1".to_string())),
            kv("key2", OTLPValue::IntValue(1)),
        ]);
        let id4 = TimeSeriesIdentity::new(vec![
            kv("key1", OTLPValue::StringValue("valueA".to_string())),
            kv("key2", OTLPValue::IntValue(1)),
        ]);

        assert!(id1 < id2);
        assert!(id2 > id1);
        assert!(id1 <= id2);
        assert!(id2 >= id1);
        assert_eq!(id1, id3);
        assert!(id1 <= id3);
        assert!(id1 >= id3);
        assert!(id1 < id4); // "value1" < "valueA"
    }

    #[test]
    #[ignore = "Unimplemented: Handle different key ID but different types"]
    fn test_compare_values_string_bool() {
        // TODO: Add test when `compare_values` handles this case
        todo!()
    }

    #[test]
    #[ignore = "Unimplemented: Handle different key ID but different types"]
    fn test_compare_values_string_int() {
        // TODO: Add test when `compare_values` handles this case
        todo!()
    }

    #[test]
    #[ignore = "Unimplemented: Handle different key ID but different types"]
    fn test_compare_values_string_double() {
        // TODO: Add test when `compare_values` handles this case
        todo!()
    }

    #[test]
    #[ignore = "Unimplemented: Handle different key ID but different types"]
    fn test_compare_values_bool_string() {
        // TODO: Add test when `compare_values` handles this case
        todo!()
    }

    #[test]
    #[ignore = "Unimplemented: Handle different key ID but different types"]
    fn test_compare_values_bool_int() {
        // TODO: Add test when `compare_values` handles this case
        todo!()
    }

    #[test]
    #[ignore = "Unimplemented: Handle different key ID but different types"]
    fn test_compare_values_bool_double() {
        // TODO: Add test when `compare_values` handles this case
        todo!()
    }

    #[test]
    #[ignore = "Unimplemented: Handle different key ID but different types"]
    fn test_compare_values_int_string() {
        // TODO: Add test when `compare_values` handles this case
        todo!()
    }

    #[test]
    #[ignore = "Unimplemented: Handle different key ID but different types"]
    fn test_compare_values_int_bool() {
        // TODO: Add test when `compare_values` handles this case
        todo!()
    }

    #[test]
    #[ignore = "Unimplemented: Handle different key ID but different types"]
    fn test_compare_values_int_double() {
        // TODO: Add test when `compare_values` handles this case
        todo!()
    }

    #[test]
    #[ignore = "Unimplemented: Handle different key ID but different types"]
    fn test_compare_values_double_string() {
        // TODO: Add test when `compare_values` handles this case
        todo!()
    }

    #[test]
    #[ignore = "Unimplemented: Handle different key ID but different types"]
    fn test_compare_values_double_bool() {
        // TODO: Add test when `compare_values` handles this case
        todo!()
    }

    #[test]
    #[ignore = "Unimplemented: Handle different key ID but different types"]
    fn test_compare_values_double_int() {
        // TODO: Add test when `compare_values` handles this case
        todo!()
    }
}
