from typing import Optional, Dict, Any
from opentelemetry._logs import LoggerProvider, Logger, LogRecord, SeverityNumber
from .common import get_exporter, now_ns

class MmapLoggerProvider(LoggerProvider):
    def __init__(self, file_path: str, resource_attributes: Optional[Dict[str, Any]] = None):
        self._exporter = get_exporter(file_path)
        self._resource_ref = self._exporter.create_resource(resource_attributes or {}, None)

    def get_logger(
        self,
        name: str,
        version: Optional[str] = None,
        schema_url: Optional[str] = None,
    ) -> Logger:
        return MmapLogger(self._exporter, self._resource_ref, name, version, schema_url)

class MmapLogger(Logger):
    def __init__(self, exporter, resource_ref, name, version, schema_url):
        self._exporter = exporter
        self._scope_ref = exporter.create_instrumentation_scope(resource_ref, name, version, {})

    def emit(self, record: LogRecord) -> None:
        # Map LogRecord to record_event
        # Use a default event name "log" for now or use body if it's a simple string.
        event_name = "log"
        
        # We need to construct attributes.
        attrs = record.attributes or {}
        
        if record.body:
             if isinstance(record.body, str):
                 event_name = record.body
             else:
                 attrs["body"] = str(record.body)
        
        severity_number = int(record.severity_number) if record.severity_number else 0
        severity_text = record.severity_text or ""
            
        ctx = None
        if record.trace_id and record.span_id:
            ctx = {
                "trace_id": record.trace_id.to_bytes(16, "big"),
                "span_id": record.span_id.to_bytes(8, "big"),
                "flags": int(record.trace_flags) if record.trace_flags else 0
            }

        self._exporter.record_event(
            self._scope_ref,
            ctx,
            event_name,
            record.timestamp or now_ns(),
            severity_number,
            severity_text,
            attrs
        )
