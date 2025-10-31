mod oltp_mmap;
mod sdk_mmap;

use oltp_mmap::{Error, OtlpMmapReader, OtlpMmapReaderConfig};
use std::path::{Path, PathBuf};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let otlp_url = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
         .unwrap_or(String::from("http://localhost:4317"));    
    // TODO - CLI argument.
    if let Ok(path) = std::env::var("OTLP_MMAP_EXPORTER_DIRECTORY")
        .map(|v| Path::new(&v).to_path_buf()) {
            return run_exporter_mmap(&otlp_url, path).await;
    }
    if let Ok(path) = std::env::var("SDK_MMAP_EXPORTER_FILE")
        .map(|v| Path::new(&v).to_path_buf()) {
            return run_sdk_mmap(&otlp_url, path).await;
    }
    Ok(())
}

async fn run_sdk_mmap(otlp_url: &str, export_file: PathBuf) -> Result<(), Error> {
    let mmap = sdk_mmap::MmapReader::new(&export_file)?;
    loop {
        tokio::select! {
            span = mmap.spans.next() => println!("Read {span:?}"),
            event = mmap.events.next() => println!("Read {event:?}"),
            measurement = mmap.events.next() => println!("Read {measurement:?}"),
        }
    }
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