use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use clap::Parser;
use otlp_mmap_collector::{
    new_collector_sdk, CollectorSdkConfig, Error, LogSdkConfig, MetricSdkConfig, TraceSdkConfig,
};

/// An MMAP Collector.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Name of the OTLP-MMAP file to read in.
    #[arg(short, long, env = "SDK_MMAP_EXPORTER_FILE")]
    input: String,

    /// The OTLP exporter endpoint to fire data into.
    #[arg(
        short,
        long,
        env = "OTEL_EXPORTER_OTLP_ENDPOINT",
        default_value = "http://localhost:4317"
    )]
    otlp_endpoint: String,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::try_parse()?;
    let path = Path::new(&args.input).to_path_buf();
    // We arbitrarily wait a few seconds for upstream to start up.
    // tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    // Wait for file to be available.
    while !path.exists() {
        println!("Waiting for {} to be available", path.display());
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
    println!("Starting SDK");
    let config = CollectorSdkConfig {
        metrics: MetricSdkConfig {
            metric_endpoint: args.otlp_endpoint.to_owned(),
            ..Default::default()
        },
        logs: LogSdkConfig {
            log_endpoint: args.otlp_endpoint.to_owned(),
            ..Default::default()
        },
        traces: TraceSdkConfig {
            trace_endpoint: args.otlp_endpoint.to_owned(),
            ..Default::default()
        },
    };
    run_sdk_mmap(&config, path).await
}

async fn run_sdk_mmap(config: &CollectorSdkConfig, export_file: PathBuf) -> Result<(), Error> {
    // TODO - configuration for reading file handling.
    let sdk = Arc::new(new_collector_sdk(&export_file)?);
    // Note: We do NOT put the different pipelines on different tasks.  We do NOT want different CPUs causing
    // cache coherency problems as this may actually slow down performance.
    let log_sdk = sdk.clone();
    let log_pipeline = async move { log_sdk.send_logs_to(&config.logs).await };
    let trace_sdk = sdk.clone();
    let trace_pipeline = async move { trace_sdk.send_traces_to(&config.traces).await };
    // We do not pass the metric piepline to another thread.
    // This is because we haven't made our aggregations "Send" yet.
    let metric_pipeline = sdk.record_metrics(&config.metrics);
    // Run the event loops by waiting on them.
    // TODO - wait for all to finish or crash?
    tokio::select! {
        r = trace_pipeline => {
            println!("Trace completed {r:?}");
            r?;
        },
        r = log_pipeline => {
            println!("Logs completed {r:?}");
            r?;
        },
        r = metric_pipeline => {
            println!("Metrics completed {r:?}");
            r?;
        },
    }
    Ok(())
}
