//! Contains components that implement a logs SDK.

use otlp_mmap_protocol::Event;
use std::{collections::HashMap, time::Duration};

use crate::{AsyncEventQueue, Error, SdkLookup};

/// A collector of events.
pub(crate) struct EventCollector {}

impl EventCollector {
    pub fn new() -> EventCollector {
        EventCollector {}
    }

    /// Batches log events and returns a new protocol request object if we have any by timeout.
    pub async fn try_create_next_batch(
        &mut self,
        reader: &impl AsyncEventQueue<Event>,
        lookup: &impl SdkLookup,
        len: usize,
        timeout: Duration,
    ) -> Result<
        Option<opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest>,
        Error,
    > {
        let buf = self.try_buffer_events(reader, lookup, len, timeout).await?;
        if !buf.is_empty() {
            return Ok(Some(self.try_create_event_batch(lookup, buf)?));
        }
        Ok(None)
    }

    fn try_create_event_batch(
        &self,
        lookup: &impl SdkLookup,
        batch: Vec<TrackedEvent>,
    ) -> Result<opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest, Error>
    {
        let mut scope_map: HashMap<i64, Vec<opentelemetry_proto::tonic::logs::v1::LogRecord>> =
            HashMap::new();
        for log in batch {
            scope_map.entry(log.scope_ref).or_default().push(log.log);
        }
        let mut resource_map: HashMap<
            i64,
            Vec<(
                i64,
                opentelemetry_proto::tonic::common::v1::InstrumentationScope,
            )>,
        > = HashMap::new();
        for scope_ref in scope_map.keys() {
            let scope = lookup.try_lookup_scope(*scope_ref)?;
            resource_map
                .entry(scope.resource_ref)
                .or_default()
                .push((*scope_ref, scope.scope));
        }
        let mut result =
            opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest {
                resource_logs: Default::default(),
            };
        for (resource_ref, scopes) in resource_map.into_iter() {
            let resource = lookup.try_lookup_resource(resource_ref)?;
            let mut resource_logs = opentelemetry_proto::tonic::logs::v1::ResourceLogs {
                resource: Some(resource),
                scope_logs: Default::default(),
                // TODO - pull this.
                schema_url: "".to_owned(),
            };
            for (sid, scope) in scopes.into_iter() {
                let mut scope_logs = opentelemetry_proto::tonic::logs::v1::ScopeLogs {
                    scope: Some(scope),
                    log_records: Vec::new(),
                    // TODO - pull this
                    schema_url: "".to_owned(),
                };
                if let Some(records) = scope_map.remove(&sid) {
                    scope_logs.log_records.extend(records);
                }
                resource_logs.scope_logs.push(scope_logs);
            }
            result.resource_logs.push(resource_logs);
        }
        Ok(result)
    }

    /// Pulls in log events and buffers them for export.
    async fn try_buffer_events(
        &mut self,
        reader: &impl AsyncEventQueue<Event>,
        lookup: &impl SdkLookup,
        len: usize,
        timeout: tokio::time::Duration,
    ) -> Result<Vec<TrackedEvent>, Error> {
        // TODO - check sanity on the file before continuing.
        // Here we create a batch of spans.
        // println!("Buffering log events");
        let mut buf = Vec::new();
        let send_by_time = tokio::time::sleep_until(tokio::time::Instant::now() + timeout);
        tokio::pin!(send_by_time);
        loop {
            tokio::select! {
                event = reader.try_read_next() => {
                    // println!("Received log event");
                    let e = self.try_handle_log_event(event?, lookup)?;
                    buf.push(e);
                    // TODO - configure the size of this.
                    if buf.len() >= len {
                        return Ok(buf)
                    }
                },
                () = &mut send_by_time => {
                    return Ok(buf)
                }
            }
        }
    }

    fn try_handle_log_event(
        &mut self,
        e: Event,
        lookup: &impl SdkLookup,
    ) -> Result<TrackedEvent, Error> {
        let event_name = if e.event_name_ref == 0 {
            "".to_owned()
        } else {
            lookup.try_read_string(e.event_name_ref)?
        };
        let (flags, trace_id, span_id) = match e.span_context {
            Some(ctx) => (ctx.flags, ctx.trace_id, ctx.span_id),
            _ => (0, Vec::new(), Vec::new()),
        };
        let body = if let Some(v) = e.body {
            lookup.try_convert_anyvalue(v)?
        } else {
            None
        };
        let mut attributes = Vec::new();
        for kv in e.attributes {
            attributes.push(lookup.try_convert_attribute(kv)?);
        }
        Ok(TrackedEvent {
            scope_ref: e.scope_ref,
            log: opentelemetry_proto::tonic::logs::v1::LogRecord {
                time_unix_nano: e.time_unix_nano,
                observed_time_unix_nano: e.time_unix_nano,
                severity_number: e.severity_number,
                severity_text: e.severity_text,
                body,
                attributes,
                dropped_attributes_count: 0,
                flags,
                trace_id,
                span_id,
                event_name,
            },
        })
    }
}

/// Tracks current status of an event from the logging ringbuffer.
pub(crate) struct TrackedEvent {
    /// Index into scope to use.
    pub scope_ref: i64,
    /// The log itself.
    pub log: opentelemetry_proto::tonic::logs::v1::LogRecord,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{MockSdkLookup, TestEventQueue};
    use otlp_mmap_core::PartialScope;
    use otlp_mmap_protocol::{any_value::Value, AnyValue, KeyValueRef};

    #[tokio::test]
    async fn test_log_conversion_and_batching() -> Result<(), Error> {
        let mut lookup = MockSdkLookup::new();
        lookup.strings.insert(1, "body_text".to_owned());
        lookup.strings.insert(2, "attr_key".to_owned());

        lookup.scopes.insert(
            10,
            PartialScope {
                resource_ref: 100,
                scope: opentelemetry_proto::tonic::common::v1::InstrumentationScope {
                    name: "scope_name".to_owned(),
                    version: "1.0".to_owned(),
                    ..Default::default()
                },
            },
        );

        lookup.resources.insert(
            100,
            opentelemetry_proto::tonic::resource::v1::Resource {
                ..Default::default()
            },
        );

        let event = Event {
            scope_ref: 10,
            time_unix_nano: 1000,
            severity_number: 9, // Info
            severity_text: "INFO".to_owned(),
            body: Some(AnyValue {
                value: Some(Value::ValueRef(1)),
            }),
            event_name_ref: 0,
            span_context: None,
            attributes: vec![KeyValueRef {
                key_ref: 2,
                value: Some(AnyValue {
                    value: Some(Value::IntValue(42)),
                }),
            }],
        };

        let queue = TestEventQueue::new(vec![event]);
        let mut collector = EventCollector::new();

        let batch = collector
            .try_create_next_batch(&queue, &lookup, 1, Duration::from_secs(1))
            .await?
            .unwrap();

        assert_eq!(batch.resource_logs.len(), 1);
        let resource_log = &batch.resource_logs[0];
        assert_eq!(resource_log.scope_logs.len(), 1);
        let scope_log = &resource_log.scope_logs[0];
        assert_eq!(scope_log.log_records.len(), 1);
        let record = &scope_log.log_records[0];

        assert_eq!(record.time_unix_nano, 1000);
        assert_eq!(record.severity_number, 9);
        assert_eq!(record.severity_text, "INFO");
        if let Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) =
            record.body.as_ref().and_then(|b| b.value.as_ref())
        {
            assert_eq!(s, "body_text");
        } else {
            panic!("Expected string body");
        }
        assert_eq!(record.attributes.len(), 1);
        assert_eq!(record.attributes[0].key, "attr_key");

        Ok(())
    }

    #[tokio::test]
    async fn test_log_grouping() -> Result<(), Error> {
        let mut lookup = MockSdkLookup::new();

        // Resource 1, Scope 1
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
        // Resource 1, Scope 2
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
        // Resource 2, Scope 3
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

        let events = vec![
            Event {
                scope_ref: 1,
                ..Default::default()
            },
            Event {
                scope_ref: 1,
                ..Default::default()
            },
            Event {
                scope_ref: 2,
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
            .unwrap();

        // Should have 2 resource logs (Resource 100 and 200)
        assert_eq!(batch.resource_logs.len(), 2);

        let res100 = batch
            .resource_logs
            .iter()
            .find(|rl| {
                rl.resource.as_ref().unwrap().attributes[0]
                    .value
                    .as_ref()
                    .unwrap()
                    .value
                    == Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(1))
            })
            .unwrap();
        assert_eq!(res100.scope_logs.len(), 2); // scope1 and scope2

        let s1 = res100
            .scope_logs
            .iter()
            .find(|sl| sl.scope.as_ref().unwrap().name == "scope1")
            .unwrap();
        assert_eq!(s1.log_records.len(), 2);

        let s2 = res100
            .scope_logs
            .iter()
            .find(|sl| sl.scope.as_ref().unwrap().name == "scope2")
            .unwrap();
        assert_eq!(s2.log_records.len(), 1);

        let res200 = batch
            .resource_logs
            .iter()
            .find(|rl| {
                rl.resource.as_ref().unwrap().attributes[0]
                    .value
                    .as_ref()
                    .unwrap()
                    .value
                    == Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(2))
            })
            .unwrap();
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
