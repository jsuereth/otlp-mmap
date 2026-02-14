//! Configuration for SDK-MMAP Collector

/// Configuration for the mmap collector .
#[derive(Default, Debug)]
pub struct CollectorSdkConfig {
    pub metrics: MetricSdkConfig,
    pub logs: LogSdkConfig,
    pub traces: TraceSdkConfig,
}

/// Metric SDK Configuration
#[derive(Debug)]
pub struct MetricSdkConfig {
    /// The reporting interval for metrics.
    pub report_interval: tokio::time::Duration,
    /// OTLP endpoit to fire metrics at.
    pub metric_endpoint: String,
}

/// Log SDK Configuration
#[derive(Debug)]
pub struct LogSdkConfig {
    /// Maximum length of log batches
    pub max_batch_length: usize,
    /// The maximum wait time before sending a log batch.
    pub batch_timeout: tokio::time::Duration,
    /// OTLP endpoit to fire metrics at.
    pub log_endpoint: String,
}

/// Trace SDK Configuration
#[derive(Debug)]
pub struct TraceSdkConfig {
    /// OTLP endpoit to fire metrics at.
    pub trace_endpoint: String,
    /// Maximum length of span batches
    pub max_batch_length: usize,
    /// The maximum wait time before sending a span batch.
    pub batch_timeout: tokio::time::Duration,
}

const DEFAULT_OTLP_ENDPOINT: &str = "http://localhost:4317";

impl Default for MetricSdkConfig {
    fn default() -> Self {
        Self {
            report_interval: tokio::time::Duration::from_mins(1),
            metric_endpoint: DEFAULT_OTLP_ENDPOINT.to_owned(),
        }
    }
}
impl Default for LogSdkConfig {
    fn default() -> Self {
        Self {
            max_batch_length: 1000,
            batch_timeout: tokio::time::Duration::from_mins(1),
            log_endpoint: DEFAULT_OTLP_ENDPOINT.to_owned(),
        }
    }
}

impl Default for TraceSdkConfig {
    fn default() -> Self {
        Self {
            max_batch_length: 1000,
            batch_timeout: tokio::time::Duration::from_mins(1),
            trace_endpoint: DEFAULT_OTLP_ENDPOINT.to_owned(),
        }
    }
}

