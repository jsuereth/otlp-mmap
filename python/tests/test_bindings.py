import otlp_mmap_internal as otlp_mmap
import pytest
import os
import tempfile

@pytest.fixture
def exporter():
    # Use a temp file path
    # On Windows, we can't open a file that is already open if we are not careful, 
    # but tempfile.NamedTemporaryFile keeps it open.
    # So we close it first.
    f = tempfile.NamedTemporaryFile(delete=False)
    path = f.name
    f.close()
    
    try:
        exp = otlp_mmap.create_otlp_mmap_exporter(path)
        yield exp
    finally:
        try:
            os.remove(path)
        except OSError:
            pass

def test_record_string(exporter):
    ref = exporter.record_string("test_string")
    assert isinstance(ref, int)
    assert ref > 0

def test_create_resource(exporter):
    ref = exporter.create_resource({"service.name": "my-service"}, None)
    assert isinstance(ref, int)

def test_instrumentation_scope(exporter):
    res = exporter.create_resource({}, None)
    scope = exporter.create_instrumentation_scope(res, "my.scope", "1.0", {"attr": "val"})
    assert isinstance(scope, int)

def test_metrics(exporter):
    res = exporter.create_resource({}, None)
    scope = exporter.create_instrumentation_scope(res, "my.scope", None, None)
    
    agg = {"gauge": {}}
    metric = exporter.create_metric_stream(scope, "my.metric", "desc", "1", agg)
    assert isinstance(metric, int)
    
    exporter.record_measurement(metric, {"tag": "val"}, 123456789, 42.0, None)

def test_events(exporter):
    res = exporter.create_resource({}, None)
    scope = exporter.create_instrumentation_scope(res, "my.scope", None, None)
    event_name = exporter.record_string("my.event")
    
    exporter.record_event(scope, None, event_name, 123456789, {"attr": "val"})

def test_spans(exporter):
    res = exporter.create_resource({}, None)
    scope = exporter.create_instrumentation_scope(res, "my.scope", None, None)
    
    trace_id = b"1234567890123456"
    span_id = b"12345678"
    
    exporter.record_span_start(scope, trace_id, span_id, None, 0, "my-span", 1, 1000, {"span.attr": 1})
    exporter.record_span_end(scope, trace_id, span_id, 2000)
