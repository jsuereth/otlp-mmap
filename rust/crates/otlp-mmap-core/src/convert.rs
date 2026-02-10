//! Helpers that can read dictionary items and auto-convert them to OTLP format.

use crate::Dictionary;
use crate::Error;

/// Helper that can convert dictionary lookups from OTLP-MMAP into OTLP.
pub struct OtlpDictionary(Dictionary);

impl OtlpDictionary {
    /// Constructs a new OTLP Dictionary.
    pub(crate) fn new(d: Dictionary) -> OtlpDictionary {
        Self(d)
    }

    /// Perform a resource lookup, including attribute lookups / conversion, for a resource.
    pub fn try_lookup_resource(
        &self,
        resource_ref: i64,
    ) -> Result<opentelemetry_proto::tonic::resource::v1::Resource, Error> {
        let resource: otlp_mmap_protocol::Resource = self.0.try_read(resource_ref)?;
        let mut attributes = Vec::new();
        for kv in resource.attributes {
            attributes.push(self.try_convert_kv(kv)?);
        }
        Ok(opentelemetry_proto::tonic::resource::v1::Resource {
            attributes,
            dropped_attributes_count: resource.dropped_attributes_count,
            // TODO - support entities.
            entity_refs: Vec::new(),
        })
    }

    /// Looks up the scope from the dictionary.
    ///
    /// Returns a "PartialScope" which is an OTLP InstrumentationScope and the reference to
    /// the resource this scope belongs to.
    pub fn try_lookup_scope(&self, scope_ref: i64) -> Result<PartialScope, Error> {
        let scope: otlp_mmap_protocol::InstrumentationScope = self.0.try_read(scope_ref)?;
        let mut attributes = Vec::new();
        for kv in scope.attributes {
            attributes.push(self.try_convert_kv(kv)?);
        }
        let name: String = self.0.try_read_string(scope.name_ref)?;
        let version: String = self.0.try_read_string(scope.version_ref)?;
        Ok(PartialScope {
            scope: opentelemetry_proto::tonic::common::v1::InstrumentationScope {
                name,
                version,
                attributes,
                dropped_attributes_count: scope.dropped_attributes_count,
            },
            resource_ref: scope.resource_ref,
        })
    }

    /// Looks up a metric definition from the dictionary.
    ///
    /// Returns the otlp_mmap_protocol Metric specification, instead of OTLP.
    ///
    /// In OTLP-MMAP all measurements MUST be aggregated, so we return the raw 'config' for how to do so.
    pub fn try_lookup_metric_stream(
        &self,
        metric_ref: i64,
    ) -> Result<otlp_mmap_protocol::MetricRef, Error> {
        self.0.try_read(metric_ref)
    }

    /// Converts a vector of OTLP-MMAP KeyValueRef into a vector of OTLP KeyValues.
    pub fn try_lookup_attributes(
        &self,
        attributes: Vec<otlp_mmap_protocol::KeyValueRef>,
    ) -> Result<Vec<opentelemetry_proto::tonic::common::v1::KeyValue>, Error> {
        attributes
            .into_iter()
            .map(|kvr| self.try_convert_kv(kvr))
            .collect()
    }

    // converts an OTLP-MMAP KeyValueRef to an OTLP KeyValue.
    fn try_convert_kv(
        &self,
        kvr: otlp_mmap_protocol::KeyValueRef,
    ) -> Result<opentelemetry_proto::tonic::common::v1::KeyValue, Error> {
        let key = self.0.try_read_string(kvr.key_ref)?;
        let value = if let Some(v) = kvr.value {
            self.try_convert_anyvalue(v)?
        } else {
            None
        };
        Ok(opentelemetry_proto::tonic::common::v1::KeyValue { key, value })
    }

    // converts an OTLP-MMAP AnyValue to an OTLP AnyValue.
    fn try_convert_anyvalue(
        &self,
        value: otlp_mmap_protocol::AnyValue,
    ) -> Result<Option<opentelemetry_proto::tonic::common::v1::AnyValue>, Error> {
        let result = match value.value {
            Some(otlp_mmap_protocol::any_value::Value::StringValue(v)) => {
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(v))
            }
            Some(otlp_mmap_protocol::any_value::Value::BoolValue(v)) => {
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(v))
            }
            Some(otlp_mmap_protocol::any_value::Value::IntValue(v)) => {
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(v))
            }
            Some(otlp_mmap_protocol::any_value::Value::DoubleValue(v)) => {
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(v))
            }
            Some(otlp_mmap_protocol::any_value::Value::BytesValue(v)) => {
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BytesValue(v))
            }
            Some(otlp_mmap_protocol::any_value::Value::ArrayValue(v)) => {
                let mut values = Vec::new();

                for av in v.values {
                    if let Some(rav) = self.try_convert_anyvalue(av)? {
                        values.push(rav);
                    }
                }
                Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::ArrayValue(
                        opentelemetry_proto::tonic::common::v1::ArrayValue { values },
                    ),
                )
            }
            Some(otlp_mmap_protocol::any_value::Value::KvlistValue(kvs)) => {
                let mut values = Vec::new();
                for kv in kvs.values {
                    values.push(self.try_convert_kv(kv)?);
                }
                Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::KvlistValue(
                        opentelemetry_proto::tonic::common::v1::KeyValueList { values },
                    ),
                )
            }
            Some(otlp_mmap_protocol::any_value::Value::ValueRef(idx)) => {
                // TODO - try to improve performance here.
                let v: otlp_mmap_protocol::AnyValue = self.0.try_read(idx)?;
                self.try_convert_anyvalue(v)?.and_then(|v| v.value)
            }
            None => None,
        };
        Ok(result
            .map(|value| opentelemetry_proto::tonic::common::v1::AnyValue { value: Some(value) }))
    }
}

/// A scope with reference to its resource in the dictionary.
pub struct PartialScope {
    /// The instrumentation scope.
    pub scope: opentelemetry_proto::tonic::common::v1::InstrumentationScope,
    /// Reference to a resource in the dictionary.
    pub resource_ref: i64,
}
