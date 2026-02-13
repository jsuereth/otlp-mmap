//! Configuration for creating OTLP-MMAP Files
//!
//! Mostly used for writing.

/// Default number of buffers in a ring.
const DEFAULT_NUM_BUFFERS: usize = 1024;
/// Default size in bytes for a buffer in a ring.
const DEFAULT_BUFFER_SIZE: usize = 512;
/// Minimum size in bytes to allocate for the dictionary in the MMAP file.
const MIN_DICTIONARY_SIZE: u64 = 1024;

/// Configuration for a RingBuffer in OTLP-MMAP.
#[derive(Debug, Clone)]
pub struct RingBufferConfig {
    /// The number of buffers in the ring.
    pub num_buffers: usize,
    /// The size, in bytes, of a buffer in the ring.
    pub buffer_size: usize,
}

/// Configuration for writing a Dictionary in OTLP-MMAP.
#[derive(Debug, Clone)]
pub struct DictionaryConfig {
    /// The size, in bytes, to initialize the dictionary in the file.
    pub initial_size: u64,
}

#[derive(Debug, Default, Clone)]
pub struct OtlpMmapConfig {
    pub events: RingBufferConfig,
    pub spans: RingBufferConfig,
    pub measurements: RingBufferConfig,
    pub dictionary: DictionaryConfig,
}

impl Default for RingBufferConfig {
    fn default() -> Self {
        Self {
            num_buffers: DEFAULT_NUM_BUFFERS,
            buffer_size: DEFAULT_BUFFER_SIZE,
        }
    }
}

impl Default for DictionaryConfig {
    fn default() -> Self {
        Self {
            initial_size: MIN_DICTIONARY_SIZE,
        }
    }
}
