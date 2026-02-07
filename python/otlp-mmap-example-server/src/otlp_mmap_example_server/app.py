import os
import sys
from typing import Optional
from flask import Flask
from opentelemetry import trace, metrics
from opentelemetry.sdk.resources import Resource
from opentelemetry.instrumentation.flask import FlaskInstrumentor

# Configuration
MMAP_FILE = os.environ.get("SDK_MMAP_EXPORTER_FILE")
OTLP_ENDPOINT = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
HTTP_PORT = int(os.environ.get("HTTP_ENDPOINT_PORT", "5000"))
SERVICE_NAME = os.environ.get("OTLP_MMAP_SERVICE_NAME", "flask-example-server")

def configure_otel():
    resource = Resource.create({
        "service.name": SERVICE_NAME,
        "service.instance.id": os.environ.get("HOSTNAME", "localhost"),
    })

    if MMAP_FILE:
        print(f"Using OTLP MMAP Exporter. File: {MMAP_FILE}")
        from otlp_mmap_sdk.metrics import MmapMeterProvider
        from otlp_mmap_sdk.trace import MmapTracerProvider
        
        # Convert BoundedAttributes to dict for Mmap providers
        resource_attrs_dict = dict(resource.attributes)

        tracer_provider = MmapTracerProvider(file_path=MMAP_FILE, resource_attributes=resource_attrs_dict)
        trace.set_tracer_provider(tracer_provider)

        meter_provider = MmapMeterProvider(file_path=MMAP_FILE, resource_attributes=resource_attrs_dict)
        metrics.set_meter_provider(meter_provider)
        
    elif OTLP_ENDPOINT:
        print(f"Using Standard OTLP Exporter. Endpoint: {OTLP_ENDPOINT}")
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

        # Tracer
        tracer_provider = TracerProvider(resource=resource)
        span_exporter = OTLPSpanExporter(endpoint=OTLP_ENDPOINT, insecure=True) 
        tracer_provider.add_span_processor(BatchSpanProcessor(span_exporter))
        trace.set_tracer_provider(tracer_provider)

        # Meter
        metric_reader = PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=OTLP_ENDPOINT, insecure=True))
        meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
        metrics.set_meter_provider(meter_provider)
    else:
        # Fallback for testing if neither is set? 
        # Or if imported by test, maybe we rely on test setting env var.
        # But if running as script, we error.
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
    app = create_app()
    app.run(host="0.0.0.0", port=HTTP_PORT)