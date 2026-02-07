import threading
import time
from typing import Dict, Optional
import otlp_mmap_internal

_exporters: Dict[str, "otlp_mmap_internal.OtlpMmapExporter"] = {}
_lock = threading.Lock()

def get_exporter(file_path: str) -> "otlp_mmap_internal.OtlpMmapExporter":
    """
    Get or create a singleton exporter for the given file path.
    This ensures that multiple providers (Meter, Tracer) sharing the same mmap file
    use the same exporter instance, which is thread-safe.
    """
    with _lock:
        if file_path not in _exporters:
            _exporters[file_path] = otlp_mmap_internal.create_otlp_mmap_exporter(file_path)
        return _exporters[file_path]

def now_ns() -> int:
    return time.time_ns()
