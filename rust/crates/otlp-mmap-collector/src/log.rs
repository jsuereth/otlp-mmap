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
