mod oltp_mmap;

use oltp_mmap::Error;
use oltp_mmap::OtlpInputAsync;
use oltp_mmap::OtlpInputCommon;
use opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue;
use std::path::Path;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let otlp_url =
        std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").unwrap_or(
            String::from("http://localhost:4317"));
            //String::from("http://[::1]:4317"));
    let path = Path::new("..").join("export");
    let input = OtlpInputAsync::new(&path)?;
    input.send_traces_to(&otlp_url).await
}

fn synchronous_read_example() {
    let path = Path::new("..").join("export");
    println!("Reading {path:?}");
    let mut otlp = OtlpInputCommon::new(&path).expect("Failed to open OTLP input");

    if !otlp.is_sane() {
        panic!("Version mismatch in OTLP export files!!!");
    }

    // TODO - Try to send the trace data via OTLP.
    // Create message channels, per-resource or scope perhaps.
    let mut idx = 0;
    loop {
        println!("Reading message #: {idx}");
        let span = otlp.next_span().expect("Failed to read next span");
        // sleep(time::Duration::from_secs(1));
        let resource = otlp
            .resource(span.resource)
            .expect("Failed to find resource");
        let scope = otlp.scope(span.scope).expect("Failed to find scope");

        if let StringValue(service_name) = resource
            .attributes
            .first()
            .unwrap()
            .value
            .as_ref()
            .unwrap()
            .value
            .as_ref()
            .unwrap()
        {
            println!(
                "Read span: {}, from {}, with {}={}",
                span.span.name,
                scope.name,
                resource.attributes.first().unwrap().key,
                service_name
            );
        }
        idx += 1;
    }
}
