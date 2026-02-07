import os
from typing import Optional
from flask import Flask
from opentelemetry import trace, metrics
from opentelemetry.sdk.resources import Resource
from opentelemetry.instrumentation.flask import FlaskInstrumentor

# Import our custom providers
from otlp_mmap_sdk.metrics import MmapMeterProvider
from otlp_mmap_sdk.trace import MmapTracerProvider

# Configuration (default if not overridden by create_app param)
_MMAP_FILE_DEFAULT = "/tmp/mmap_otel_data"
SERVICE_NAME = os.environ.get("OTLP_MMAP_SERVICE_NAME", "flask-example-server")
MMAP_FILE = _MMAP_FILE_DEFAULT # Global variable for easier debugging/access if needed

def create_app(mmap_file_path: Optional[str] = None):
    global MMAP_FILE
    if mmap_file_path:
        MMAP_FILE = mmap_file_path
    else:
        MMAP_FILE = os.environ.get("OTLP_MMAP_FILE", _MMAP_FILE_DEFAULT)

    print(f"Flask App starting. MMAP_FILE: {MMAP_FILE}")
    # Configure Resource
    resource = Resource.create({
        "service.name": SERVICE_NAME,
        "service.instance.id": os.environ.get("HOSTNAME", "localhost"),
    })

    # Convert BoundedAttributes to dict
    resource_attrs_dict = dict(resource.attributes)

    # Configure TracerProvider
    tracer_provider = MmapTracerProvider(file_path=MMAP_FILE, resource_attributes=resource_attrs_dict)
    print(f"TracerProvider created. ResourceRef: {tracer_provider._resource_ref}")
    trace.set_tracer_provider(tracer_provider)
    print("TracerProvider set.")

    # Configure MeterProvider
    meter_provider = MmapMeterProvider(file_path=MMAP_FILE, resource_attributes=resource_attrs_dict)
    print(f"MeterProvider created. ResourceRef: {meter_provider._resource_ref}")
    metrics.set_meter_provider(meter_provider)
    print("MeterProvider set.")

    # Instrument Flask app
    app = Flask(__name__)
    FlaskInstrumentor().instrument_app(app)

    # Get tracer and meter
    tracer = trace.get_tracer(__name__)
    meter = metrics.get_meter(__name__)
    
    # Create a counter
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
    app.run(host="0.0.0.0", port=5000)
