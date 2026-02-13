import pytest
import tempfile
import os
import sys
import subprocess
import otlp_mmap_internal
from otlp_mmap_sdk.metrics import MmapMeterProvider
from otlp_mmap_sdk.trace import MmapTracerProvider

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

@pytest.fixture(scope="module")
def proto_classes():
    # Ensure grpcio-tools is available
    try:
        import grpc_tools.protoc
    except ImportError:
        pytest.skip("grpcio-tools not installed")

    with tempfile.TemporaryDirectory() as tmp_dir:
        proto_file = "/app/specification/mmap.proto"
        if not os.path.exists(proto_file):
            # Fallback for local testing relative path
            proto_file = os.path.join(os.path.dirname(__file__), "../../../../specification/mmap.proto")
            if not os.path.exists(proto_file):
                pytest.skip(f"mmap.proto not found at {proto_file}")
        
        proto_dir = os.path.dirname(proto_file)
        
        cmd = [
            sys.executable, "-m", "grpc_tools.protoc",
            f"-I{proto_dir}",
            f"--python_out={tmp_dir}",
            proto_file
        ]
        subprocess.check_call(cmd)
        
        sys.path.append(tmp_dir)
        import mmap_pb2
        yield mmap_pb2
        sys.path.remove(tmp_dir)

def test_readback_metrics(mmap_file, proto_classes):
    # Write data
    provider = MmapMeterProvider(mmap_file)
    meter = provider.get_meter("my.meter")
    counter = meter.create_counter("my.counter")
    counter.add(10, {"attr": "val"})
    
    # Read back
    reader = otlp_mmap_internal.MmapReader(mmap_file)
    
    found = False
    for _ in range(100):
        data = reader.read_metric()
        if data:
            measurement = proto_classes.Measurement()
            measurement.ParseFromString(data)
            
            # Current SDK writes as_double
            if measurement.HasField("as_double"):
                assert measurement.as_double == 10.0
                found = True
                # Check attributes? 
                # Attributes are KeyValueRef which reference dictionary.
                # We need to read dictionary to resolve.
                # Reader currently doesn't expose dictionary reading in Python efficiently yet 
                # (it exposes the dict object but we haven't added lookup methods to PyMmapReader).
                break
            elif measurement.HasField("as_long"):
                assert measurement.as_long == 10
                found = True
                break
            
    assert found

def test_readback_spans(mmap_file, proto_classes):
    provider = MmapTracerProvider(mmap_file)
    tracer = provider.get_tracer("my.tracer")
    
    with tracer.start_as_current_span("my-span") as span:
        span.set_attribute("key", "value")
    
    reader = otlp_mmap_internal.MmapReader(mmap_file)
    
    found_start = False
    found_end = False
    
    for _ in range(100):
        data = reader.read_span()
        if data:
            event = proto_classes.SpanEvent()
            event.ParseFromString(data)
            
            if event.HasField("start"):
                assert event.start.name == "my-span"
                found_start = True
            elif event.HasField("end"):
                found_end = True
                
        if found_start and found_end:
            break
            
    assert found_start
    assert found_end