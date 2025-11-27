mod sdk_mmap;

use sdk_mmap::Error;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::sdk_mmap::CollectorSdk;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let otlp_url = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or(String::from("http://localhost:4317"));
    // TODO - CLI arguments.
    if let Ok(path) = std::env::var("SDK_MMAP_EXPORTER_FILE").map(|v| Path::new(&v).to_path_buf()) {
        // println!("Waiting for {} to be available", path.display());
        // // We arbitrarily wait a few seconds for upstream to start up.
        // tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
        // Wait for file to be available.
        while !path.exists() {
            println!("Waiting for {} to be available", path.display());
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
        println!("Starting SDK");
        return run_sdk_mmap(&otlp_url, path).await;
    }
    Ok(())
}

async fn run_sdk_mmap(otlp_url: &str, export_file: PathBuf) -> Result<(), Error> {
    let sdk = Arc::new(CollectorSdk::new(&export_file)?);
    // let metric_pipeline = tokio::task::spawn(async move { metric_sdk.record_metrics(&metric_otlp).await });
    let log_otlp = otlp_url.to_owned();
    let log_sdk = sdk.clone();
    let log_pipeline = tokio::task::spawn(async move { log_sdk.send_logs_to(&log_otlp).await });
    let trace_otlp = otlp_url.to_owned();
    let trace_sdk = sdk.clone();
    let trace_pipeline =
        tokio::task::spawn(async move { trace_sdk.send_traces_to(&trace_otlp).await });
    // We do not pass the metric piepline to another thread.
    // This is because we haven't made our aggregations "Send" yet.
    let metric_pipeline = sdk.record_metrics(&otlp_url);
    // Run the event loops by waiting on them.
    // TODO - wait for all to finish or crash?
    tokio::select! {
        r = trace_pipeline => {
            println!("Trace completed {:?}", r);
            let _ = r?;
        },
        r = log_pipeline => {
            println!("Logs completed {:?}", r);
            let _ = r?;
        },
        r = metric_pipeline => {
            println!("Metrics completed {:?}", r);
            let _ = r?;
        },
    }
    Ok(())
}
