from typing import Optional, Iterable, Sequence, Dict, Any, Union
from opentelemetry.metrics import (
    MeterProvider,
    Meter,
    Counter,
    Histogram,
    ObservableCounter,
    ObservableGauge,
    UpDownCounter,
    ObservableUpDownCounter,
    CallbackT,
    NoOpMeter
)
from .common import get_exporter, now_ns

class MmapMeterProvider(MeterProvider):
    def __init__(self, file_path: str, resource_attributes: Optional[Dict[str, Any]] = None):
        self._exporter = get_exporter(file_path)
        # Create resource
        # TODO: Handle standard resource attributes (service.name etc) if not provided?
        self._resource_ref = self._exporter.create_resource(resource_attributes or {}, None)

    def get_meter(
        self,
        name: str,
        version: Optional[str] = None,
        schema_url: Optional[str] = None,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> Meter:
        return MmapMeter(self._exporter, self._resource_ref, name, version, schema_url, attributes)

class MmapMeter(Meter):
    def __init__(self, exporter, resource_ref, name, version, schema_url, attributes=None):
        self._exporter = exporter
        # TODO: version and schema_url mapping?
        self._scope_ref = exporter.create_instrumentation_scope(resource_ref, name, version, attributes or {})

    def create_counter(self, name: str, unit: str = "", description: str = "") -> Counter:
        return MmapCounter(self._exporter, self._scope_ref, name, unit, description)

    def create_up_down_counter(self, name: str, unit: str = "", description: str = "") -> UpDownCounter:
        return MmapUpDownCounter(self._exporter, self._scope_ref, name, unit, description)

    def create_histogram(self, name: str, unit: str = "", description: str = "") -> Histogram:
        return MmapHistogram(self._exporter, self._scope_ref, name, unit, description)

    def create_observable_counter(
        self, name: str, callbacks: Optional[Sequence[CallbackT]] = None, unit: str = "", description: str = ""
    ) -> ObservableCounter:
        # TODO: Handle callbacks. For now just create the stream.
        return MmapObservableCounter(self._exporter, self._scope_ref, name, unit, description, callbacks)

    def create_observable_gauge(
        self, name: str, callbacks: Optional[Sequence[CallbackT]] = None, unit: str = "", description: str = ""
    ) -> ObservableGauge:
        return MmapObservableGauge(self._exporter, self._scope_ref, name, unit, description, callbacks)

    def create_observable_up_down_counter(
        self, name: str, callbacks: Optional[Sequence[CallbackT]] = None, unit: str = "", description: str = ""
    ) -> ObservableUpDownCounter:
        return MmapObservableUpDownCounter(self._exporter, self._scope_ref, name, unit, description, callbacks)

class MmapInstrument:
    def __init__(self, exporter, scope_ref, name, unit, description, aggregation):
        self._exporter = exporter
        self._metric_ref = exporter.create_metric_stream(scope_ref, name, description, unit, aggregation)

    def _record(self, value: Union[int, float], attributes: Optional[Dict[str, Any]] = None):
        self._exporter.record_measurement(self._metric_ref, attributes or {}, now_ns(), float(value), None)

class MmapCounter(MmapInstrument, Counter):
    def __init__(self, exporter, scope_ref, name, unit, description):
        # Delta Sum, Monotonic
        agg = {"sum": {"aggregation_temporality": 1, "is_monotonic": True}} # 1=Delta
        super().__init__(exporter, scope_ref, name, unit, description, agg)

    def add(self, amount: Union[int, float], attributes: Optional[Dict[str, Any]] = None) -> None:
        if amount < 0:
            return
        self._record(amount, attributes)

class MmapUpDownCounter(MmapInstrument, UpDownCounter):
    def __init__(self, exporter, scope_ref, name, unit, description):
        # Delta Sum, Non-Monotonic
        agg = {"sum": {"aggregation_temporality": 1, "is_monotonic": False}}
        super().__init__(exporter, scope_ref, name, unit, description, agg)

    def add(self, amount: Union[int, float], attributes: Optional[Dict[str, Any]] = None) -> None:
        self._record(amount, attributes)

class MmapHistogram(MmapInstrument, Histogram):
    def __init__(self, exporter, scope_ref, name, unit, description):
        # Histogram, Delta
        agg = {"histogram": {"aggregation_temporality": 1, "bucket_boundaries": []}} # Default buckets?
        super().__init__(exporter, scope_ref, name, unit, description, agg)

    def record(self, amount: Union[int, float], attributes: Optional[Dict[str, Any]] = None, context: Optional[Any] = None) -> None:
        self._record(amount, attributes)

class MmapObservableCounter(MmapInstrument, ObservableCounter):
    def __init__(self, exporter, scope_ref, name, unit, description, callbacks):
        # Cumulative Sum? Observables usually report cumulative state?
        # Or Delta? 
        # API says: "Observable instruments are asynchronous...".
        # If the callback returns the *current* value (e.g. CPU usage), it is effectively a Gauge or Cumulative Sum.
        # But if it returns monotonic increment, it's cumulative.
        # Let's assume Cumulative Sum for Counter.
        agg = {"sum": {"aggregation_temporality": 2, "is_monotonic": True}} # 2=Cumulative
        super().__init__(exporter, scope_ref, name, unit, description, agg)
        self._callbacks = callbacks
        # TODO: Register callbacks execution mechanism.

class MmapObservableGauge(MmapInstrument, ObservableGauge):
    def __init__(self, exporter, scope_ref, name, unit, description, callbacks):
        agg = {"gauge": {}}
        super().__init__(exporter, scope_ref, name, unit, description, agg)
        self._callbacks = callbacks

class MmapObservableUpDownCounter(MmapInstrument, ObservableUpDownCounter):
    def __init__(self, exporter, scope_ref, name, unit, description, callbacks):
        agg = {"sum": {"aggregation_temporality": 2, "is_monotonic": False}} # Cumulative
        super().__init__(exporter, scope_ref, name, unit, description, agg)
        self._callbacks = callbacks
