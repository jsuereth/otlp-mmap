mod oltp_mmap;

use oltp_mmap::{Error,OtlpMmapReader};
use std::path::Path;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let otlp_url = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or(String::from("http://localhost:4317"));
    //String::from("http://[::1]:4317"));
    let path = Path::new("..").join("export");
    let input = OtlpMmapReader::new(&path)?;
    input.send_traces_to(&otlp_url).await
}
