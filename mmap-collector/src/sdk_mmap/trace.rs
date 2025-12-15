//! Contains components that implement the necessary pieces of the OpenTelemetry tracing SDK on the collection side of the mmap.
//!
//! This should expect incoming SpanEvents from the OTLP-MMAP protocol, and generate TrackedSpan structures when these spans complete.
//! The `ActiveSpans` struct is used to store spans in-flight, and may contain spans which will never complete and must be removed
//! after some time.

use crate::sdk_mmap::{
    data::{span_event::Event, SpanEvent},
    ringbuffer::AsyncEventQueue,
    AttributeLookup, Error,
};
use std::collections::HashMap;
/// An efficient mechanism to hash and lookup spans.
#[derive(Clone, Copy, Hash, PartialEq, Eq)]
struct FullSpanId {
    trace_id: [u8; 16],
    span_id: [u8; 8],
}
impl FullSpanId {
    fn try_from_event(e: &SpanEvent) -> Result<FullSpanId, Error> {
        Ok(FullSpanId {
            trace_id: e.trace_id.as_slice().try_into()?,
            span_id: e.span_id.as_slice().try_into()?,
        })
    }
}
/// Used for debugging trace/span ids.
fn bytes_to_hex_string(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{byte:02x}")) // Format each byte as a two-digit lowercase hex
        .collect() // Collect the formatted strings into a single String
}

impl std::fmt::Display for FullSpanId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "span {} @ {}",
            bytes_to_hex_string(&self.trace_id),
            bytes_to_hex_string(&self.span_id)
        )
    }
}

/// Tracks current status of a span from span events.
///
/// TODO - This should likely track last seen timestamp for GC
///        and possibly be used for error reporting.
pub(crate) struct TrackedSpan {
    // Index into scope to use.
    pub scope_ref: i64,
    pub current: opentelemetry_proto::tonic::trace::v1::Span,
}

/// A tracker of active spans from span events.
pub(crate) struct ActiveSpans {
    /// A cache of all active spans that have not seen an `end` event.
    spans: HashMap<FullSpanId, TrackedSpan>,
}
// TODO - move more OTLP handling code here?
impl ActiveSpans {
    /// Constructs a new Active span tracker.
    pub fn new() -> ActiveSpans {
        ActiveSpans {
            spans: HashMap::new(),
        }
    }

    /// Returns the number of active spans.
    pub fn num_active(&self) -> usize {
        self.spans.len()
    }

    /// Reads events, tracking spans and attempts to construct a buffer.
    ///
    /// If timeout is met before buffer is filled, the buffer is returned.
    pub async fn try_buffer_spans<
        Q: AsyncEventQueue<SpanEvent> + Sync,
        L: AttributeLookup + Sync,
    >(
        &mut self,
        event_queue: &Q,
        lookup: &L,
        len: usize,
        timeout: tokio::time::Duration,
    ) -> Result<Vec<TrackedSpan>, Error> {
        // TODO - check sanity on the file before continuing.
        // Here we create a batch of spans.
        let mut buf = Vec::new();
        let send_by_time = tokio::time::sleep_until(tokio::time::Instant::now() + timeout);
        tokio::pin!(send_by_time);
        loop {
            // println!("Waiting for span event");
            tokio::select! {
                event = event_queue.try_read_next() => {
                    // println!("Received span event");
                    if let Some(span) = self.try_handle_span_event(event?, lookup).await? {
                        // println!("Buffering span");
                        buf.push(span);
                        // TODO - configure the size of this.
                        if buf.len() >= len {
                            return Ok(buf)
                        }
                    }
                },
                () = &mut send_by_time => {
                    // println!("Got timeout waiting for span event");
                    return Ok(buf)
                }
            }
        }
    }

    /// Handles a span event.
    ///
    /// Returns a span, if this event has completed it.
    async fn try_handle_span_event<AL: AttributeLookup + Sync>(
        &mut self,
        e: SpanEvent,
        attr_lookup: &AL,
    ) -> Result<Option<TrackedSpan>, Error> {
        let hash = FullSpanId::try_from_event(&e)?;
        // println!("Span event: {hash}");
        match e.event {
            Some(Event::Start(start)) => {
                // TODO - optimise attribute load
                let mut attributes = Vec::new();
                for kvr in start.attributes {
                    attributes.push(attr_lookup.try_convert_attribute(kvr).await?);
                }
                let span_state = opentelemetry_proto::tonic::trace::v1::Span {
                    trace_id: e.trace_id,
                    span_id: e.span_id,
                    // TODO - make sure we record trace state.
                    trace_state: "".into(),
                    parent_span_id: start.parent_span_id,
                    flags: start.flags,
                    name: start.name,
                    kind: start.kind,
                    start_time_unix_nano: start.start_time_unix_nano,
                    attributes,
                    // Things we don't have yet.
                    end_time_unix_nano: 0,
                    dropped_attributes_count: 0,
                    events: Vec::new(),
                    dropped_events_count: 0,
                    links: Vec::new(),
                    dropped_links_count: 0,
                    status: None,
                };
                self.spans.insert(
                    hash,
                    TrackedSpan {
                        current: span_state,
                        scope_ref: e.scope_ref,
                    },
                );
            }
            Some(Event::Link(_)) => todo!(),
            Some(Event::Name(ne)) => {
                if let Some(entry) = self.spans.get_mut(&hash) {
                    entry.current.name = ne.name;
                }
            }
            Some(Event::Attributes(ae)) => {
                // TODO - optimise attribute load
                if let Some(entry) = self.spans.get_mut(&hash) {
                    for kvr in ae.attributes {
                        entry
                            .current
                            .attributes
                            .push(attr_lookup.try_convert_attribute(kvr).await?);
                    }
                }
            }
            Some(Event::End(se)) => {
                if let Some(mut entry) = self.spans.remove(&hash) {
                    entry.current.end_time_unix_nano = se.end_time_unix_nano;
                    if let Some(status) = se.status {
                        entry.current.status = Some(opentelemetry_proto::tonic::trace::v1::Status {
                            message: status.message,
                            code: status.code,
                        })
                    }
                    return Ok(Some(entry));
                }
            }
            // Log the issue vs. crash.
            None => todo!("logic error!"),
        }
        // TODO - garbage collection if dangling spans is too high?
        Ok(None)
    }
}

#[cfg(test)]
mod test {
    use crate::sdk_mmap::data::any_value::Value;
    use crate::sdk_mmap::data::span_event::{EndSpan, Event, StartSpan};
    use crate::sdk_mmap::data::SpanEvent;
    use crate::sdk_mmap::data::{AnyValue, KeyValueRef, Status};
    use crate::sdk_mmap::ringbuffer::AsyncEventQueue;
    use crate::sdk_mmap::trace::ActiveSpans;
    use crate::sdk_mmap::AttributeLookup;
    use crate::sdk_mmap::Error;
    use std::collections::HashMap;
    use tokio::sync::Mutex;

    struct TestSpanEventQueue {
        index: Mutex<usize>,
        events: Vec<SpanEvent>,
    }

    impl TestSpanEventQueue {
        fn new<E: Into<Vec<SpanEvent>>>(events: E) -> Self {
            Self {
                index: Mutex::new(0),
                events: events.into(),
            }
        }
    }
    impl AsyncEventQueue<SpanEvent> for TestSpanEventQueue {
        async fn try_read_next(&self) -> Result<crate::sdk_mmap::data::SpanEvent, Error> {
            let mut idx = self.index.lock().await;
            if *idx < self.events.len() {
                let real_idx: usize = *idx;
                *idx += 1;
                Ok(self.events[real_idx].to_owned())
            } else {
                // TODO - real error
                Err(Error::NotFoundInDictionary(
                    "Index is too large".to_owned(),
                    *idx as i64,
                ))
            }
        }
    }
    // TODO - move this into some helper location for all SDK pieces.
    struct TestAttributeLookup {
        string_lookup: HashMap<i64, String>,
    }
    impl TestAttributeLookup {
        fn new(string_lookup: HashMap<i64, String>) -> Self {
            Self { string_lookup }
        }
    }

    impl AttributeLookup for TestAttributeLookup {
        async fn try_convert_attribute(
            &self,
            kv: crate::sdk_mmap::data::KeyValueRef,
        ) -> Result<opentelemetry_proto::tonic::common::v1::KeyValue, Error> {
            // TODO - share this definition with actual algorithm.
            let key = self
                .string_lookup
                .get(&kv.key_ref)
                .cloned()
                .unwrap_or("<not found>".to_owned());
            // TODO - handle real conversions.
            let value = match kv.value {
                None => None,
                Some(v) => match v.value {
                    None => None,
                    Some(Value::IntValue(v)) => {
                        Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                            value: Some(
                                opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(
                                    v,
                                ),
                            ),
                        })
                    }
                    Some(v) => todo!("Support value {v:?}"),
                },
            };
            Ok(opentelemetry_proto::tonic::common::v1::KeyValue { key, value })
        }
    }

    #[tokio::test]
    async fn test_active_spans_returns_completed_span() -> Result<(), Error> {
        let attr = TestAttributeLookup::new([(1, "one".to_owned())].into_iter().collect());
        let mut tracker = ActiveSpans::new();
        let scope_ref = 10i64;
        let trace_id: Vec<u8> = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let span_id: Vec<u8> = vec![0, 1, 2, 3, 4, 5, 6, 7];
        let parent_span_id: Vec<u8> = vec![7, 6, 5, 4, 3, 2, 1, 0];
        let start = SpanEvent {
            scope_ref,
            trace_id: trace_id.clone(),
            span_id: span_id.clone(),
            event: Some(Event::Start(StartSpan {
                parent_span_id: parent_span_id.clone(),
                flags: 0,
                name: "name".to_owned(),
                kind: 1,
                start_time_unix_nano: 1,
                // TODO - test attribute conversion.
                attributes: vec![KeyValueRef {
                    key_ref: 1,
                    value: Some(AnyValue {
                        value: Some(Value::IntValue(1)),
                    }),
                }],
            })),
        };
        let result = tracker.try_handle_span_event(start, &attr).await?;
        assert!(
            result.is_none(),
            "Should not return complete span on start event"
        );
        let end = SpanEvent {
            scope_ref,
            trace_id: trace_id.clone(),
            span_id: span_id.clone(),
            event: Some(Event::End(EndSpan {
                end_time_unix_nano: 10,
                status: Some(Status {
                    message: "Test status".to_owned(),
                    code: 2,
                }),
            })),
        };
        let result2 = tracker.try_handle_span_event(end, &attr).await?;
        assert!(
            result2.is_some(),
            "Should return complete span after span end."
        );
        if let Some(span) = result2 {
            assert_eq!(span.scope_ref, scope_ref);
            assert_eq!(span.current.trace_id, trace_id);
            assert_eq!(span.current.span_id, span_id);
            assert_eq!(span.current.parent_span_id, parent_span_id);
            assert_eq!(span.current.start_time_unix_nano, 1);
            assert_eq!(span.current.end_time_unix_nano, 10);
            assert_eq!(span.current.kind, 1);
            assert_eq!(span.current.name, "name");
            assert!(span.current.status.is_some());
            assert_eq!(span.current.attributes.len(), 1);
            if let Some(attr) = span.current.attributes.first() {
                assert_eq!(attr.key, "one");
            }
        }
        Ok(())
    }

    fn make_span_id(i: u8) -> Vec<u8> {
        vec![0, 1, 2, 3, 4, 5, 6, i]
    }

    fn make_span_start(span_id: Vec<u8>) -> SpanEvent {
        SpanEvent {
            scope_ref: 1,
            trace_id: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            span_id,
            event: Some(Event::Start(StartSpan {
                parent_span_id: Vec::new(),
                flags: 0,
                name: "name".to_owned(),
                kind: 1,
                start_time_unix_nano: 1,
                // TODO - test attribute conversion.
                attributes: vec![],
            })),
        }
    }
    fn make_span_end(span_id: Vec<u8>) -> SpanEvent {
        SpanEvent {
            scope_ref: 1,
            trace_id: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            span_id,
            event: Some(Event::End(EndSpan {
                end_time_unix_nano: 10,
                status: Some(Status {
                    message: "Test status".to_owned(),
                    code: 2,
                }),
            })),
        }
    }

    #[tokio::test]
    async fn test_active_spans_create_batch() -> Result<(), Error> {
        let attr = TestAttributeLookup::new([(1, "one".to_owned())].into_iter().collect());
        let mut tracker = ActiveSpans::new();
        let event_queue = TestSpanEventQueue::new([
            make_span_start(make_span_id(0)),
            make_span_start(make_span_id(1)),
            make_span_end(make_span_id(1)),
            make_span_start(make_span_id(3)),
            make_span_end(make_span_id(0)),
            make_span_start(make_span_id(2)),
            make_span_end(make_span_id(3)),
            make_span_end(make_span_id(2)),
        ]);
        let batch = tracker
            .try_buffer_spans(&event_queue, &attr, 3, tokio::time::Duration::from_secs(10))
            .await?;
        assert_eq!(batch.len(), 3, "Failed to batch three spans");
        // We should have a remaining active span.
        assert_eq!(tracker.num_active(), 1);
        Ok(())
    }
}
