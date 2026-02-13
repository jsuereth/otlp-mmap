from typing import Optional, Dict, Any, Union, Sequence, Iterator
from contextlib import contextmanager
from opentelemetry.trace import (
    TracerProvider,
    Tracer,
    Span,
    SpanContext,
    SpanKind,
    Status,
    StatusCode,
    Link,
    NonRecordingSpan,
    TraceFlags
)
from opentelemetry import trace, context as context_api
from .common import get_exporter, now_ns
import os
import secrets

class MmapTracerProvider(TracerProvider):
    def __init__(self, file_path: str, resource_attributes: Optional[Dict[str, Any]] = None):
        self._exporter = get_exporter(file_path)
        self._resource_ref = self._exporter.create_resource(resource_attributes or {}, None)

    def get_tracer(
        self,
        instrumenting_module_name: str,
        instrumenting_library_version: Optional[str] = None,
        schema_url: Optional[str] = None,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> Tracer:
        return MmapTracer(self._exporter, self._resource_ref, instrumenting_module_name, instrumenting_library_version, schema_url, attributes)

class MmapTracer(Tracer):
    def __init__(self, exporter, resource_ref, name, version, schema_url, attributes=None):
        self._exporter = exporter
        self._scope_ref = exporter.create_instrumentation_scope(resource_ref, name, version, attributes or {})

    def start_span(
        self,
        name: str,
        context: Optional[context_api.Context] = None,
        kind: SpanKind = SpanKind.INTERNAL,
        attributes: Optional[Dict[str, Any]] = None,
        links: Sequence[Link] = None,
        start_time: Optional[int] = None,
        record_exception: bool = True,
        set_status_on_exception: bool = True,
    ) -> Span:
        # Generate IDs
        # trace_id: 16 bytes
        # span_id: 8 bytes
        
        # Check context for parent
        parent_span_context = trace.get_current_span(context).get_span_context() if context else None
        
        if parent_span_context and parent_span_context.is_valid:
            trace_id_bytes = parent_span_context.trace_id.to_bytes(16, "big")
            parent_span_id_bytes = parent_span_context.span_id.to_bytes(8, "big")
            # Trace flags?
        else:
            trace_id_bytes = secrets.token_bytes(16)
            parent_span_id_bytes = None

        span_id_bytes = secrets.token_bytes(8)
        
        # Convert IDs to int for SpanContext (used by OTel API)
        trace_id_int = int.from_bytes(trace_id_bytes, "big")
        span_id_int = int.from_bytes(span_id_bytes, "big")
        
        span_context = SpanContext(
            trace_id=trace_id_int,
            span_id=span_id_int,
            is_remote=False,
            trace_flags=TraceFlags.SAMPLED, # Assume sampled for now?
        )
        
        return MmapSpan(
            name=name,
            context=span_context,
            exporter=self._exporter,
            scope_ref=self._scope_ref,
            kind=kind,
            attributes=attributes,
            links=links,
            start_time=start_time or now_ns(),
            parent_span_id=parent_span_id_bytes,
            trace_id_bytes=trace_id_bytes,
            span_id_bytes=span_id_bytes
        )

    @contextmanager
    def start_as_current_span(
        self,
        name: str,
        context: Optional[context_api.Context] = None,
        kind: SpanKind = SpanKind.INTERNAL,
        attributes: Optional[Dict[str, Any]] = None,
        links: Sequence[Link] = None,
        start_time: Optional[int] = None,
        record_exception: bool = True,
        set_status_on_exception: bool = True,
        end_on_exit: bool = True,
    ) -> Iterator[Span]:
        span = self.start_span(
            name=name,
            context=context,
            kind=kind,
            attributes=attributes,
            links=links,
            start_time=start_time,
            record_exception=record_exception,
            set_status_on_exception=set_status_on_exception,
        )
        with trace.use_span(span, end_on_exit=end_on_exit):
            yield span

class MmapSpan(Span):
    def __init__(self, name, context, exporter, scope_ref, kind, attributes, links, start_time, parent_span_id, trace_id_bytes, span_id_bytes):
        super().__init__()
        self._context = context
        self._exporter = exporter
        self._scope_ref = scope_ref
        self._trace_id_bytes = trace_id_bytes
        self._span_id_bytes = span_id_bytes
        self._end_time = None
        self._kind = kind # Store kind
        
        # Record start
        # Map kind enum to int
        # Internal=1, Server=2, Client=3, Producer=4, Consumer=5
        kind_int = 1 # Default Internal
        if kind == SpanKind.SERVER: kind_int = 2
        elif kind == SpanKind.CLIENT: kind_int = 3
        elif kind == SpanKind.PRODUCER: kind_int = 4
        elif kind == SpanKind.CONSUMER: kind_int = 5
        
        # Flags?
        flags = 0
        if context.trace_flags & TraceFlags.SAMPLED:
            flags |= 1
            
        self._exporter.record_span_start(
            scope_ref,
            trace_id_bytes,
            span_id_bytes,
            parent_span_id,
            flags,
            name,
            kind_int,
            start_time,
            attributes or {}
        )

    def get_span_context(self) -> SpanContext:
        return self._context

    @property
    def kind(self) -> SpanKind:
        return self._kind
    
    def end(self, end_time: Optional[int] = None) -> None:
        if self._end_time is not None:
            return
        self._end_time = end_time or now_ns()
        self._exporter.record_span_end(
            self._scope_ref,
            self._trace_id_bytes,
            self._span_id_bytes,
            self._end_time
        )

    def is_recording(self) -> bool:
        return True

    def add_event(
        self,
        name: str,
        attributes: Optional[Dict[str, Any]] = None,
        timestamp: Optional[int] = None,
    ) -> None:
        # Create span context dict for export
        sc_dict = {
            "trace_id": self._trace_id_bytes,
            "span_id": self._span_id_bytes,
            "flags": 1 # Sampled
        }
        self._exporter.record_event(
            self._scope_ref,
            sc_dict,
            name,
            timestamp or now_ns(),
            0, # severity_number
            "", # severity_text
            attributes or {}
        )

    def update_name(self, name: str) -> None:
        pass

    def set_attribute(self, key: str, value: Any) -> None:
        pass

    def set_attributes(self, attributes: Dict[str, Any]) -> None:
        pass

    def set_status(self, status: Union[Status, StatusCode], description: Optional[str] = None) -> None:
        pass

    def record_exception(
        self,
        exception: Exception,
        attributes: Optional[Dict[str, Any]] = None,
        timestamp: Optional[int] = None,
        escaped: bool = False,
    ) -> None:
        pass
