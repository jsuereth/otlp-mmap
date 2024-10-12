use std::path::Path;

use dictionary::DictionaryInputChannel;
use ringbuffer::RingbufferInputChannel;

pub mod ringbuffer;
pub mod dictionary;

/// An implementation that reads OTLP data.
pub struct OtlpInputCommon {
    resources: DictionaryInputChannel,
    scopes: DictionaryInputChannel,
    spans: RingbufferInputChannel
}

impl OtlpInputCommon {
    pub fn new(p: &Path) -> OtlpInputCommon {
        let resources = DictionaryInputChannel::new(&p.join("resource.otlp"));
        let scopes = DictionaryInputChannel::new(&p.join("scope.otlp"));
        let spans = RingbufferInputChannel::new(&p.join("spans.otlp"));
        OtlpInputCommon { resources,scopes,spans}
    }

    // TODO fn next_span(&mut self) -> OtlpSpan

    pub fn next_span(&mut self) -> OtlpSpan {
        let _ = self.spans.next();
        // TODO - read span from buf.
        OtlpSpan {}
    }
}

pub struct OtlpSpan {}