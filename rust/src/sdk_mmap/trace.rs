//! Contains components that implement the necessary pieces of tracing SDK on the collection side of the mmap.

use crate::{
    oltp_mmap::Error,
    sdk_mmap::{
        data::{span_event::Event, SpanEvent},
        CollectorSdk,
    },
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
        .map(|byte| format!("{:02x}", byte)) // Format each byte as a two-digit lowercase hex
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

    /// Reads events, tracking spans and attempts to construct a buffer.
    ///
    /// If timeout is met before buffer is filled, the buffer is returned.
    pub async fn try_buffer_spans(
        &mut self,
        sdk: &CollectorSdk,
        len: usize,
        timeout: tokio::time::Duration,
    ) -> Result<Vec<TrackedSpan>, Error> {
        // TODO - check sanity on the file before continuing.
        // Here we create a batch of spans.
        let mut buf = Vec::new();
        let send_by_time = tokio::time::sleep_until(tokio::time::Instant::now() + timeout);
        tokio::pin!(send_by_time);
        loop {
            tokio::select! {
                event = sdk.reader.spans.next() => {
                    if let Some(span) = self.try_handle_span_event(event?, sdk).await? {
                        buf.push(span);
                        // TODO - configure the size of this.
                        if buf.len() >= len {
                            return Ok(buf)
                        }
                    }
                },
                () = &mut send_by_time => {
                    return Ok(buf)
                }
            }
        }
    }

    /// Handles a span event.
    ///
    /// Returns a span, if this event has completed it.
    async fn try_handle_span_event(
        &mut self,
        e: SpanEvent,
        attr_lookup: &CollectorSdk,
    ) -> Result<Option<TrackedSpan>, Error> {
        let hash = FullSpanId::try_from_event(&e)?;
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
