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
                std::cmp::Ordering::Equal => match compare_opt_any_values(&l.value, &r.value) {
                    std::cmp::Ordering::Less => return std::cmp::Ordering::Less,
                    std::cmp::Ordering::Greater => return std::cmp::Ordering::Greater,
                    std::cmp::Ordering::Equal => (),
                },
            }
        }
        self.attributes.len().cmp(&other.attributes.len())
    }
}

fn compare_opt_any_values(
    l: &Option<opentelemetry_proto::tonic::common::v1::AnyValue>,
    r: &Option<opentelemetry_proto::tonic::common::v1::AnyValue>,
) -> std::cmp::Ordering {
    match (l, r) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(lv), Some(rv)) => compare_any_values(lv, rv),
    }
}

fn compare_any_values(
    l: &opentelemetry_proto::tonic::common::v1::AnyValue,
    r: &opentelemetry_proto::tonic::common::v1::AnyValue,
) -> std::cmp::Ordering {
    match (l.value.as_ref(), r.value.as_ref()) {
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
    
    // 1. Try to compare directly if same type for efficiency
    match (l, r) {
        (Value::StringValue(ls), Value::StringValue(rs)) => return ls.cmp(rs),
        (Value::BoolValue(lb), Value::BoolValue(rb)) => return lb.cmp(rb),
        (Value::IntValue(li), Value::IntValue(ri)) => return li.cmp(ri),
        (Value::DoubleValue(ld), Value::DoubleValue(rd)) => return ld.total_cmp(rd),
        (Value::BytesValue(lb), Value::BytesValue(rb)) => return lb.cmp(rb),
        (Value::ArrayValue(la), Value::ArrayValue(ra)) => {
            match la.values.len().cmp(&ra.values.len()) {
                std::cmp::Ordering::Equal => {
                    for (lv, rv) in la.values.iter().zip(ra.values.iter()) {
                        match compare_any_values(lv, rv) {
                            std::cmp::Ordering::Equal => continue,
                            ord => return ord,
                        }
                    }
                    return std::cmp::Ordering::Equal;
                }
                ord => return ord,
            }
        }
        (Value::KvlistValue(lk), Value::KvlistValue(rk)) => {
            match lk.values.len().cmp(&rk.values.len()) {
                std::cmp::Ordering::Equal => {
                    // For Kvlist we should ideally sort them to compare as maps,
                    // but the user suggested JSON-like strings for complex types.
                    // Fallthrough to string comparison.
                }
                ord => return ord,
            }
        }
        _ => {} 
    }

    // 2. Different types or complex types needing stable tie-breaker: compare via string representation
    let ls = value_to_string(l);
    let rs = value_to_string(r);
    ls.cmp(&rs)
}

fn value_to_string(v: &opentelemetry_proto::tonic::common::v1::any_value::Value) -> String {
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    match v {
        Value::StringValue(s) => s.clone(),
        Value::BoolValue(b) => b.to_string(),
        Value::IntValue(i) => i.to_string(),
        Value::DoubleValue(d) => d.to_string(),
        Value::BytesValue(b) => format!("{:?}", b),
        Value::ArrayValue(a) => {
            let mut s = String::from("[");
            for (i, val) in a.values.iter().enumerate() {
                if i > 0 { s.push(','); }
                s.push_str(&any_value_to_string(val));
            }
            s.push(']');
            s
        }
        Value::KvlistValue(k) => {
            let mut kvs = k.values.clone();
            kvs.sort_by(|a, b| a.key.cmp(&b.key));
            let mut s = String::from("{");
            for (i, kv) in kvs.iter().enumerate() {
                if i > 0 { s.push(','); }
                s.push_str(&kv.key);
                s.push(':');
                s.push_str(&any_value_to_string_opt(&kv.value));
            }
            s.push('}');
            s
        }
    }
}

fn any_value_to_string(v: &opentelemetry_proto::tonic::common::v1::AnyValue) -> String {
    any_value_to_string_opt(&Some(v.clone()))
}

fn any_value_to_string_opt(v: &Option<opentelemetry_proto::tonic::common::v1::AnyValue>) -> String {
    match v.as_ref().and_then(|av| av.value.as_ref()) {
        Some(val) => value_to_string(val),
        None => "null".to_owned(),
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
            kv("key1", OTLPValue::StringValue("value1".to_owned())),
            kv("key2", OTLPValue::IntValue(1)),
        ]);
        let id2 = TimeSeriesIdentity::new(vec![
            kv("key1", OTLPValue::StringValue("value1".to_owned())),
            kv("key2", OTLPValue::IntValue(2)),
        ]);
        let id3 = TimeSeriesIdentity::new(vec![
            kv("key1", OTLPValue::StringValue("value1".to_owned())),
            kv("key2", OTLPValue::IntValue(1)),
        ]);
        let id4 = TimeSeriesIdentity::new(vec![
            kv("key1", OTLPValue::StringValue("valueA".to_owned())),
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
    fn test_compare_values_string_int() {
        let s = OTLPValue::StringValue("10".to_owned());
        let i = OTLPValue::IntValue(10);
        assert_eq!(compare_values(&s, &i), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_compare_values_bool_string() {
        let b = OTLPValue::BoolValue(true);
        let s = OTLPValue::StringValue("true".to_owned());
        assert_eq!(compare_values(&b, &s), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_compare_values_mixed_complex() {
        let a = OTLPValue::ArrayValue(opentelemetry_proto::tonic::common::v1::ArrayValue {
            values: vec![OTLPAnyValue { value: Some(OTLPValue::IntValue(1)) }]
        });
        let s = OTLPValue::StringValue("[1]".to_owned());
        assert_eq!(compare_values(&a, &s), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_compare_values_kvlist_sorting() {
        let kv1 = opentelemetry_proto::tonic::common::v1::KeyValue {
            key: "a".to_owned(),
            value: Some(OTLPAnyValue { value: Some(OTLPValue::IntValue(1)) }),
        };
        let kv2 = opentelemetry_proto::tonic::common::v1::KeyValue {
            key: "b".to_owned(),
            value: Some(OTLPAnyValue { value: Some(OTLPValue::IntValue(2)) }),
        };
        
        let l1 = OTLPValue::KvlistValue(opentelemetry_proto::tonic::common::v1::KeyValueList {
            values: vec![kv1.clone(), kv2.clone()],
        });
        let l2 = OTLPValue::KvlistValue(opentelemetry_proto::tonic::common::v1::KeyValueList {
            values: vec![kv2.clone(), kv1.clone()],
        });
        
        // They should be equal because value_to_string sorts them
        assert_eq!(compare_values(&l1, &l2), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_any_value_to_string_opt_null() {
        assert_eq!(any_value_to_string_opt(&None), "null");
        assert_eq!(any_value_to_string_opt(&Some(OTLPAnyValue { value: None })), "null");
    }
}
