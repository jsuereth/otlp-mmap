//! Logic for collecting events from MMAP and converting them into OTLP log batches.

use crate::{AsyncEventQueue, Error, SdkLookup};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use otlp_mmap_protocol::Event;
use std::{collections::HashMap, time::Duration};

/// Helper to collect and group log events.
pub struct EventCollector {}

impl EventCollector {
    pub fn new() -> Self {
        Self {}
    }

    /// Attempts to read events from the queue and create a batch of OTLP logs.
    pub async fn try_create_next_batch<Q: AsyncEventQueue<Event>, L: SdkLookup>(
        &mut self,
        queue: &Q,
        lookup: &L,
        max_batch_size: usize,
        timeout: Duration,
    ) -> Result<Option<ExportLogsServiceRequest>, Error> {
        let mut events = Vec::new();
        let deadline = tokio::time::Instant::now() + timeout;

        while events.len() < max_batch_size {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() && !events.is_empty() {
                break;
            }

            tokio::select! {
                res = queue.try_read_next() => {
                    match res {
                        Ok(e) => events.push(e),
                        Err(e) => {
                            // If we hit an error (like end of queue) and have events, return them.
                            if !events.is_empty() {
                                break;
                            }
                            return Err(e);
                        }
                    }
                }
                _ = tokio::time::sleep(remaining) => {
                    break;
                }
            }
        }

        if events.is_empty() {
            return Ok(None);
        }

        Ok(Some(self.group_events(events, lookup)?))
    }

    /// Groups events by Resource -> instrumentation scope, for OTLP export request.
    fn group_events<L: SdkLookup>(
        &self,
        events: Vec<Event>,
        lookup: &L,
    ) -> Result<ExportLogsServiceRequest, Error> {
        let mut resource_map: HashMap<
            i64,
            HashMap<i64, Vec<opentelemetry_proto::tonic::logs::v1::LogRecord>>,
        > = HashMap::new();

        for event in events {
            let scope = lookup.try_lookup_scope(event.scope_ref)?;
            let mut attributes = Vec::with_capacity(event.attributes.len());
            for attr_ref in event.attributes {
                attributes.push(lookup.try_convert_attribute(attr_ref)?);
            }

            let (trace_id, span_id, flags) = if let Some(ctx) = event.span_context {
                (ctx.trace_id, ctx.span_id, ctx.flags)
            } else {
                (Vec::new(), Vec::new(), 0)
            };

            let log_record = opentelemetry_proto::tonic::logs::v1::LogRecord {
                time_unix_nano: event.time_unix_nano,
                observed_time_unix_nano: event.time_unix_nano,
                severity_number: event.severity_number,
                severity_text: event.severity_text,
                body: event
                    .body
                    .and_then(|b| lookup.try_convert_anyvalue(b).ok().flatten()),
                attributes,
                dropped_attributes_count: 0,
                flags,
                trace_id,
                span_id,
                event_name: lookup
                    .try_read_string(event.event_name_ref)
                    .unwrap_or_default(),
            };

            resource_map
                .entry(scope.resource_ref)
                .or_default()
                .entry(event.scope_ref)
                .or_default()
                .push(log_record);
        }

        let mut resource_logs = Vec::new();
        for (res_ref, scope_map) in resource_map {
            let mut scope_logs = Vec::new();
            for (scope_ref, log_records) in scope_map {
                let scope = lookup.try_lookup_scope(scope_ref)?;
                scope_logs.push(opentelemetry_proto::tonic::logs::v1::ScopeLogs {
                    scope: Some(scope.scope),
                    log_records,
                    schema_url: "".to_string(),
                });
            }

            resource_logs.push(opentelemetry_proto::tonic::logs::v1::ResourceLogs {
                resource: Some(lookup.try_lookup_resource(res_ref)?),
                scope_logs,
                schema_url: "".to_string(),
            });
        }

        Ok(ExportLogsServiceRequest { resource_logs })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{MockSdkLookup, TestEventQueue};
    use otlp_mmap_core::PartialScope;
    use otlp_mmap_protocol::{any_value::Value, AnyValue, KeyValueRef, SpanContext};

    #[tokio::test]
    async fn test_log_conversion_and_batching() -> Result<(), Error> {
        let mut lookup = MockSdkLookup::new();
        lookup.strings.insert(1, "key1".to_owned());
        lookup.strings.insert(2, "scope1".to_owned());
        lookup.strings.insert(3, "1.0".to_owned());
        lookup.strings.insert(4, "event_name".to_owned());

        lookup.resources.insert(
            1,
            opentelemetry_proto::tonic::resource::v1::Resource {
                attributes: vec![opentelemetry_proto::tonic::common::v1::KeyValue {
                    key: "key1".to_owned(),
                    value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                        value: Some(
                            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                                "res_val".to_owned(),
                            ),
                        ),
                    }),
                }],
                ..Default::default()
            },
        );

        lookup.scopes.insert(
            1,
            PartialScope {
                resource_ref: 1,
                scope: opentelemetry_proto::tonic::common::v1::InstrumentationScope {
                    name: "scope1".to_owned(),
                    version: "1.0".to_owned(),
                    ..Default::default()
                },
            },
        );

        let mut collector = EventCollector::new();
        let queue = TestEventQueue::new([Event {
            scope_ref: 1,
            time_unix_nano: 12345,
            severity_text: "INFO".to_owned(),
            severity_number: 9,
            body: Some(AnyValue {
                value: Some(Value::StringValue("log message".to_owned())),
            }),
            attributes: vec![KeyValueRef {
                key_ref: 1,
                value: Some(AnyValue {
                    value: Some(Value::StringValue("attr_val".to_owned())),
                }),
            }],
            event_name_ref: 4,
            span_context: Some(SpanContext {
                trace_id: vec![1; 16],
                span_id: vec![2; 8],
                flags: 1,
            }),
        }]);

        let batch = collector
            .try_create_next_batch(&queue, &lookup, 10, tokio::time::Duration::from_secs(1))
            .await?
            .expect("Failed to create log batch");

        assert_eq!(batch.resource_logs.len(), 1);
        let rl = &batch.resource_logs[0];
        assert_eq!(
            rl.resource
                .as_ref()
                .expect("Resource should be present")
                .attributes[0]
                .key,
            "key1"
        );
        let sl = &rl.scope_logs[0];
        assert_eq!(
            sl.scope.as_ref().expect("Scope should be present").name,
            "scope1"
        );
        let lr = &sl.log_records[0];
        assert_eq!(lr.time_unix_nano, 12345);
        assert_eq!(lr.severity_text, "INFO");
        assert_eq!(lr.attributes[0].key, "key1");
        assert_eq!(lr.event_name, "event_name");
        assert_eq!(lr.trace_id, vec![1; 16]);
        assert_eq!(lr.span_id, vec![2; 8]);
        assert_eq!(lr.flags, 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_log_grouping() -> Result<(), Error> {
        let mut lookup = MockSdkLookup::new();
        lookup.strings.insert(1, "scope1".to_owned());
        lookup.strings.insert(2, "scope2".to_owned());
        lookup.strings.insert(3, "scope3".to_owned());

        // Resource 100
        lookup.resources.insert(
            100,
            opentelemetry_proto::tonic::resource::v1::Resource {
                attributes: vec![opentelemetry_proto::tonic::common::v1::KeyValue {
                    key: "res".to_owned(),
                    value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                        value: Some(
                            opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(1),
                        ),
                    }),
                }],
                ..Default::default()
            },
        );
        // Resource 200
        lookup.resources.insert(
            200,
            opentelemetry_proto::tonic::resource::v1::Resource {
                attributes: vec![opentelemetry_proto::tonic::common::v1::KeyValue {
                    key: "res".to_owned(),
                    value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                        value: Some(
                            opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(2),
                        ),
                    }),
                }],
                ..Default::default()
            },
        );

        // Scopes
        lookup.scopes.insert(
            1,
            PartialScope {
                resource_ref: 100,
                scope: opentelemetry_proto::tonic::common::v1::InstrumentationScope {
                    name: "scope1".to_owned(),
                    ..Default::default()
                },
            },
        );
        lookup.scopes.insert(
            2,
            PartialScope {
                resource_ref: 100,
                scope: opentelemetry_proto::tonic::common::v1::InstrumentationScope {
                    name: "scope2".to_owned(),
                    ..Default::default()
                },
            },
        );
        lookup.scopes.insert(
            3,
            PartialScope {
                resource_ref: 200,
                scope: opentelemetry_proto::tonic::common::v1::InstrumentationScope {
                    name: "scope3".to_owned(),
                    ..Default::default()
                },
            },
        );

        let events = vec![
            Event {
                scope_ref: 1,
                time_unix_nano: 1,
                ..Default::default()
            },
            Event {
                scope_ref: 2,
                time_unix_nano: 2,
                ..Default::default()
            },
            Event {
                scope_ref: 1,
                time_unix_nano: 3,
                ..Default::default()
            },
            Event {
                scope_ref: 3,
                ..Default::default()
            },
        ];

        let queue = TestEventQueue::new(events);
        let mut collector = EventCollector::new();

        let batch = collector
            .try_create_next_batch(&queue, &lookup, 4, Duration::from_secs(1))
            .await?
            .expect("Failed to create log batch");

        // Should have 2 resource logs (Resource 100 and 200)
        assert_eq!(batch.resource_logs.len(), 2);

        let res100 = batch
            .resource_logs
            .iter()
            .find(|rl| {
                rl.resource
                    .as_ref()
                    .expect("Resource should be present")
                    .attributes[0]
                    .value
                    .as_ref()
                    .expect("Value should be present")
                    .value
                    == Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(1))
            })
            .expect("Resource 100 should be present in batch");
        assert_eq!(res100.scope_logs.len(), 2); // scope1 and scope2

        let s1 = res100
            .scope_logs
            .iter()
            .find(|sl| sl.scope.as_ref().expect("Scope should be present").name == "scope1")
            .expect("Scope 1 should be present in batch");
        assert_eq!(s1.log_records.len(), 2);

        let s2 = res100
            .scope_logs
            .iter()
            .find(|sl| sl.scope.as_ref().expect("Scope should be present").name == "scope2")
            .expect("Scope 2 should be present in batch");
        assert_eq!(s2.log_records.len(), 1);

        let res200 = batch
            .resource_logs
            .iter()
            .find(|rl| {
                rl.resource
                    .as_ref()
                    .expect("Resource should be present")
                    .attributes[0]
                    .value
                    .as_ref()
                    .expect("Value should be present")
                    .value
                    == Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(2))
            })
            .expect("Resource 200 should be present in batch");
        assert_eq!(res200.scope_logs.len(), 1); // scope3
        assert_eq!(res200.scope_logs[0].log_records.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_invalid_dictionary_reference() -> Result<(), Error> {
        let lookup = MockSdkLookup::new(); // Empty strings map
        let event = Event {
            scope_ref: 10,
            event_name_ref: 1, // Invalid
            ..Default::default()
        };

        let queue = TestEventQueue::new(vec![event]);
        let mut collector = EventCollector::new();

        let result = collector
            .try_create_next_batch(&queue, &lookup, 1, Duration::from_secs(1))
            .await;

        assert!(result.is_err());
        Ok(())
    }
}
