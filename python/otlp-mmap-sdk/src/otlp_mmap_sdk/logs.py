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
        # We need an event name. OTel Logs don't strictly have "event name" like the mmap protocol expects (it expects event_name_ref).
        # We can use "log" or the body as name? Or map body to body field in proto?
        # The mmap protocol `Event` has `body`, `severity_number`, etc.
        # But `OtlpMmapExporter.record_event` currently takes `event_name_ref`.
        # `data.rs` Event has `event_name_ref`.
        
        # Let's use a default event name "log" for now.
        event_name_ref = self._exporter.record_string("log")
        
        # We need to construct attributes.
        attrs = record.attributes or {}
        
        # TODO: Handle body, severity, etc.
        # The current `record_event` signature in `internal` only takes `attributes` and `event_name_ref`.
        # It doesn't expose `body` or `severity` fields of the `Event` struct in `sdk.rs`.
        # `sdk.rs` `record_event` initializes severity to 0 and body to None.
        
        # So we can only pass attributes for now.
        # We can encode body/severity into attributes if needed or update `internal` later.
        
        if record.body:
            attrs["body"] = str(record.body)
        if record.severity_text:
            attrs["severity_text"] = record.severity_text
        if record.severity_number:
            attrs["severity_number"] = int(record.severity_number)
            
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
            event_name_ref,
            record.timestamp or now_ns(),
            attrs
        )
