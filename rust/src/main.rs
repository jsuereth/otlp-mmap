mod oltp_mmap;

use oltp_mmap::{Error,OtlpMmapReader, OtlpMmapReaderConfig};
use std::path::{Path, PathBuf};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let otlp_url = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or(String::from("http://localhost:4317"));
    //String::from("http://[::1]:4317"));
    // TODO - CLI argument.
    let path: PathBuf =
        std::env::var("OTLP_MMAP_EXPORTER_DIRECTORY")
        .map(|v| Path::new(&v).to_path_buf())
        .unwrap_or(Path::new("..").join("export"));
    let input = OtlpMmapReader::new(&path, OtlpMmapReaderConfig {
        ..Default::default()
    })?;
    input.send_traces_to(&otlp_url).await
}
