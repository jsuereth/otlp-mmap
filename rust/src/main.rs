mod oltp_mmap;
mod sdk_mmap;

use oltp_mmap::{Error, OtlpMmapReader, OtlpMmapReaderConfig};
use std::path::{Path, PathBuf};

use crate::sdk_mmap::CollectorSdk;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let otlp_url = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or(String::from("http://localhost:4317"));
    // TODO - CLI argument.
    if let Ok(path) =
        std::env::var("OTLP_MMAP_EXPORTER_DIRECTORY").map(|v| Path::new(&v).to_path_buf())
    {
        return run_exporter_mmap(&otlp_url, path).await;
    }
    if let Ok(path) = std::env::var("SDK_MMAP_EXPORTER_FILE").map(|v| Path::new(&v).to_path_buf()) {
        // Wait for file to be available.
        while !path.exists() {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
        return run_sdk_mmap(&otlp_url, path).await;
    }
    Ok(())
}

async fn run_sdk_mmap(otlp_url: &str, export_file: PathBuf) -> Result<(), Error> {
    let sdk = CollectorSdk::new(&export_file)?;
    // Create our event loops to handle things.
    let trace_loop = sdk.send_traces_to(otlp_url);
    let event_loop = sdk.send_logs_to(otlp_url);
    let metric_loop = sdk.dev_null_metrics();

    // Run the event loops by waiting on them.
    // If we return, then we are "done", we propagate the errors.
    tokio::select! {
        r = trace_loop => {
            let _ = r?;
        },
        r = event_loop => {
            let _ = r?;
        },
        r = metric_loop => {
            let _ = r?;
        },
    }
    Ok(())
}

async fn run_exporter_mmap(otlp_url: &str, exporter_dir: PathBuf) -> Result<(), Error> {
    let input = OtlpMmapReader::new(
        &exporter_dir,
        OtlpMmapReaderConfig {
            ..Default::default()
        },
    )?;
    input.send_traces_to(&otlp_url).await
}
