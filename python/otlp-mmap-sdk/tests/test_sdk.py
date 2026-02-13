import pytest
import tempfile
import os
import time
from otlp_mmap_sdk.metrics import MmapMeterProvider
from otlp_mmap_sdk.trace import MmapTracerProvider
from otlp_mmap_sdk.logs import MmapLoggerProvider
from opentelemetry._logs import LogRecord
from opentelemetry.metrics import Observation

@pytest.fixture
def mmap_file():
    f = tempfile.NamedTemporaryFile(delete=False)
    path = f.name
    f.close()
    yield path
    try:
        os.remove(path)
    except:
        pass

def test_metrics(mmap_file):
    provider = MmapMeterProvider(mmap_file)
    meter = provider.get_meter("my.meter")
    counter = meter.create_counter("my.counter")
    counter.add(10, {"attr": "val"})

def test_async_metrics(mmap_file):
    # Set shorter interval for testing if possible, but we can call _collect manually
    provider = MmapMeterProvider(mmap_file)
    meter = provider.get_meter("my.meter")
    
    def observable_callback(options):
        yield Observation(42, {"async": "true"})
        
    meter.create_observable_counter("my.async.counter", callbacks=[observable_callback])
    
    # Force collection
    provider._collector._collect()
    
    # We can't easily verify the mmap content without a reader, but we can ensure it doesn't crash.

def test_tracing(mmap_file):
    provider = MmapTracerProvider(mmap_file)
    tracer = provider.get_tracer("my.tracer")
    with tracer.start_as_current_span("my-span") as span:
        # span.set_attribute("key", "value") # Not implemented yet in MmapSpan
        span.add_event("something happened")

def test_logging(mmap_file):
    provider = MmapLoggerProvider(mmap_file)
    logger = provider.get_logger("my.logger")
    
    record = LogRecord(
        timestamp=123456789,
        trace_id=0,
        span_id=0,
        trace_flags=None,
        severity_text="INFO",
        severity_number=9,
        body="hello",
        attributes={"a": "b"}
    )
    logger.emit(record)
