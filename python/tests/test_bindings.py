import otlp_mmap_internal as otlp_mmap
import pytest
import os
import tempfile

@pytest.fixture
def exporter():
    # Use a temp file path
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
    
    exporter.record_event(scope, None, "my.event", 123456789, 0, "", {"attr": "val"})

def test_spans(exporter):
    res = exporter.create_resource({}, None)
    scope = exporter.create_instrumentation_scope(res, "my.scope", None, None)
    
    trace_id = b"1234567890123456"
    span_id = b"12345678"
    
    exporter.record_span_start(scope, trace_id, span_id, None, 0, "my-span", 1, 1000, {"span.attr": 1})
    exporter.record_span_end(scope, trace_id, span_id, 2000)

def test_caching(exporter):
    # Resource caching
    res1 = exporter.create_resource({"a": 1, "b": 2}, None)
    res2 = exporter.create_resource({"b": 2, "a": 1}, None)
    assert res1 == res2, "Resource caching failed"

    # Scope caching
    scope1 = exporter.create_instrumentation_scope(res1, "scope", "1.0", {"x": "y"})
    scope2 = exporter.create_instrumentation_scope(res1, "scope", "1.0", {"x": "y"})
    assert scope1 == scope2, "Scope caching failed"

    # Metric caching
    agg = {"sum": {"aggregation_temporality": 1, "is_monotonic": True}}
    m1 = exporter.create_metric_stream(scope1, "metric", "desc", "unit", agg)
    m2 = exporter.create_metric_stream(scope1, "metric", "desc", "unit", agg)
    assert m1 == m2, "Metric caching failed"
