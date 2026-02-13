import os
import sys
from typing import Optional
from flask import Flask
from opentelemetry import trace, metrics
from opentelemetry.sdk.resources import Resource
from opentelemetry.instrumentation.flask import FlaskInstrumentor

def configure_otel():
    # Read configuration from env vars inside the function to allow overrides/mocking
    mmap_file = os.environ.get("SDK_MMAP_EXPORTER_FILE")
    otlp_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
    service_name = os.environ.get("OTLP_MMAP_SERVICE_NAME", "flask-example-server")

    resource = Resource.create({
        "service.name": service_name,
        "service.instance.id": os.environ.get("HOSTNAME", "localhost"),
    })

    if mmap_file:
        print(f"Using OTLP MMAP Exporter. File: {mmap_file}")
        from otlp_mmap_sdk.metrics import MmapMeterProvider
        from otlp_mmap_sdk.trace import MmapTracerProvider
        
        # Convert BoundedAttributes to dict for Mmap providers
        resource_attrs_dict = dict(resource.attributes)

        tracer_provider = MmapTracerProvider(file_path=mmap_file, resource_attributes=resource_attrs_dict)
        trace.set_tracer_provider(tracer_provider)

        meter_provider = MmapMeterProvider(file_path=mmap_file, resource_attributes=resource_attrs_dict)
        metrics.set_meter_provider(meter_provider)
        
    elif otlp_endpoint:
        print(f"Using Standard OTLP Exporter. Endpoint: {otlp_endpoint}")
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

        # Tracer
        tracer_provider = TracerProvider(resource=resource)
        span_exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True) 
        tracer_provider.add_span_processor(BatchSpanProcessor(span_exporter))
        trace.set_tracer_provider(tracer_provider)

        # Meter
        metric_reader = PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=otlp_endpoint, insecure=True))
        meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
        metrics.set_meter_provider(meter_provider)
    else:
        if "pytest" not in sys.modules:
             sys.exit("Must provide either an OTEL_EXPORTER_OTLP_ENDPOINT or SDK_MMAP_EXPORTER_FILE env var.")
        else:
             print("Warning: No exporter configured (running in test mode?)")

def create_app():
    configure_otel()
    
    app = Flask(__name__)
    FlaskInstrumentor().instrument_app(app)

    tracer = trace.get_tracer(__name__)
    meter = metrics.get_meter(__name__)
    
    requests_counter = meter.create_counter(
        name="requests_total",
        description="Total number of requests",
        unit="1"
    )

    @app.route("/")
    def hello():
        requests_counter.add(1, {"endpoint": "/"})
        with tracer.start_as_current_span("hello-world-span"):
            return "Hello, World!"

    @app.route("/fib/<int:n>")
    def fibonacci(n):
        with tracer.start_as_current_span("fibonacci-calculation") as span:
            span.set_attribute("fib.n", n)
            result = _fib(n)
            span.set_attribute("fib.result", result)
            requests_counter.add(1, {"endpoint": "/fib"})
            return f"Fibonacci({n}) = {result}"

    def _fib(n):
        if n <= 1:
            return n
        return _fib(n - 1) + _fib(n - 2)
        
    return app

if __name__ == "__main__":
    http_port = int(os.environ.get("HTTP_ENDPOINT_PORT", "5000"))
    app = create_app()
    app.run(host="0.0.0.0", port=http_port)
