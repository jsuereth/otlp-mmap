import otlp_mmap
import pytest

def test_functions_exist():
    assert hasattr(otlp_mmap, "record_string")
    assert hasattr(otlp_mmap, "create_resource")
    assert hasattr(otlp_mmap, "create_instrumentation_scope")
    assert hasattr(otlp_mmap, "create_metric_stream")
    assert hasattr(otlp_mmap, "record_measurement")
    assert hasattr(otlp_mmap, "record_event")
    assert hasattr(otlp_mmap, "record_span_start")
    assert hasattr(otlp_mmap, "record_span_end")
