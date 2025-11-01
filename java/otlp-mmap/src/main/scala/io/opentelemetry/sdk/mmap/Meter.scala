package io.opentelemetry.sdk.mmap

import io.opentelemetry.api.metrics.DoubleGaugeBuilder
import io.opentelemetry.api.metrics.DoubleHistogramBuilder
import io.opentelemetry.api.metrics.LongUpDownCounterBuilder
import io.opentelemetry.api.metrics.LongCounterBuilder
import java.util.function.Consumer
import io.opentelemetry.api.metrics.DoubleCounterBuilder
import io.opentelemetry.api.metrics.ObservableLongCounter
import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.metrics.ObservableLongMeasurement
import io.opentelemetry.api.metrics.DoubleCounter
import io.opentelemetry.api.metrics.ObservableDoubleCounter
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement
import io.opentelemetry.sdk.mmap.internal.data.MetricDictionary
import opentelemetry.proto.mmap.v1.{Mmap=>MmapProto}
import io.opentelemetry.sdk.mmap.internal.RingBuffer
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.context.Context
import java.time.Instant
import io.opentelemetry.sdk.mmap.internal.data.StringDictionary
import io.opentelemetry.sdk.mmap.internal.data.AttributeHelper
import io.opentelemetry.api.metrics.MeterBuilder
import io.opentelemetry.sdk.mmap.internal.data.ScopeDictionary
import io.opentelemetry.sdk.mmap.internal.Dictionary
import io.opentelemetry.api.metrics.LongHistogramBuilder
import io.opentelemetry.api.metrics.DoubleHistogram
import io.opentelemetry.sdk.mmap.internal.SdkMmapRaw


class MeterProvider(state: MeterProviderState) extends io.opentelemetry.api.metrics.MeterProvider:
  override def meterBuilder(instrumentationScopeName: String): MeterBuilder =
    MeterBuilder(instrumentationScopeName, state)

case class MeterProviderState(
  resource_ref: Long,
  mmap: SdkMmapRaw)

class MeterBuilder(name: String, provider_state: MeterProviderState) extends io.opentelemetry.api.metrics.MeterBuilder:
  private var version = ""
  private var schema_url = ""
  
  override def setInstrumentationVersion(instrumentationScopeVersion: String): io.opentelemetry.api.metrics.MeterBuilder = 
    version = instrumentationScopeVersion
    this
  override def setSchemaUrl(schemaUrl: String): io.opentelemetry.api.metrics.MeterBuilder =
    schema_url = schemaUrl
    this
  override def build(): io.opentelemetry.api.metrics.Meter =
    val sid = provider_state.mmap.scopes.intern(provider_state.resource_ref, name, version, schema_url, Attributes.empty())
    val meter_state = MeterSharedState(sid, provider_state.mmap)
    Meter(meter_state)

case class MeterSharedState(
  scopeId: Long,
  mmap: SdkMmapRaw)

/// Implementation of meter that records metric definitions in a dictionary, and measurements in ringbuffer.
class Meter(state: MeterSharedState) extends io.opentelemetry.api.metrics.Meter:

  override def counterBuilder(name: String): LongCounterBuilder = 
    val m = MmapProto.MetricRef.newBuilder()
    m.setSum(MmapProto.MetricRef.Sum.newBuilder()
    // TODO - pull temporality from SDK view config?
    .setAggregationTemporality(MmapProto.AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE)
    .setIsMonotonic(true)
    .build())
    LongCounterBuilder(state, m)

  override def upDownCounterBuilder(name: String): LongUpDownCounterBuilder = ???

  override def histogramBuilder(name: String): DoubleHistogramBuilder =
    val m = MmapProto.MetricRef.newBuilder()
    m.setExpHist(MmapProto.MetricRef.ExponentialHistogram.newBuilder()
    .setAggregationTemporality(MmapProto.AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE)
    .setMaxScale(20)
    .setMaxBuckets(20)
    )
    DoubleHistogramBuilder(m, state)

  override def gaugeBuilder(name: String): DoubleGaugeBuilder = ???


class LongCounterBuilder(state: MeterSharedState, metric: opentelemetry.proto.mmap.v1.Mmap.MetricRef.Builder) extends io.opentelemetry.api.metrics.LongCounterBuilder:
  override def setUnit(unit: String): io.opentelemetry.api.metrics.LongCounterBuilder =
    metric.setUnit(unit)
    this
  override def setDescription(description: String): io.opentelemetry.api.metrics.LongCounterBuilder =
      metric.setDescription(description)
      this

  override def ofDoubles(): DoubleCounterBuilder =
    DoubleCounterBuilder(state, metric)

  override def build(): LongCounter = 
    // TODO - memoize the metric
    val mid = state.mmap.metrics.intern(metric.build())
    LongCounter(mid, state.mmap)

  override def buildWithCallback(callback: Consumer[ObservableLongMeasurement]): ObservableLongCounter = 
    // TODO - register with something that will issue callbacks.
    ???


class LongCounter(metric_ref: Long, mmap: SdkMmapRaw) extends io.opentelemetry.api.metrics.LongCounter:
  override def add(value: Long): Unit = add(value, Attributes.empty())
  override def add(value: Long, attributes: Attributes): Unit = add(value, attributes, Context.current())
  override def add(value: Long, attributes: Attributes, context: Context): Unit =
    val m = MmapProto.Measurement.newBuilder()
    m.setAsLong(value)
    m.setMetricRef(metric_ref)
    // TODO - more efficient attribute handling
    attributes.forEach((k,v) => {
      m.addAttributes(AttributeHelper.convertKv(mmap.strings)(k,v))
    })
    m.setSpanContext(internal.convertContext(context))
    // TODO - more efficient clock
    m.setTimeUnixNano(internal.convertInstant(Instant.now()))
    import internal.data.given
    mmap.measurements.write(m.build())

class DoubleCounterBuilder(state: MeterSharedState, metric: opentelemetry.proto.mmap.v1.Mmap.MetricRef.Builder)
extends io.opentelemetry.api.metrics.DoubleCounterBuilder
// TODO - Use attribute advice.
with io.opentelemetry.api.incubator.metrics.ExtendedDoubleCounterBuilder:
  override def setUnit(unit: String): io.opentelemetry.api.metrics.DoubleCounterBuilder = 
    metric.setUnit(unit)
    this
  override def setDescription(description: String): io.opentelemetry.api.metrics.DoubleCounterBuilder =
    metric.setDescription(description)
    this

  override def buildWithCallback(callback: Consumer[ObservableDoubleMeasurement]): ObservableDoubleCounter =
    ???

  override def build(): DoubleCounter =
    val mid = state.mmap.metrics.intern(metric.build())
    DoubleCounter(mid, state.mmap)

class DoubleCounter(metric_ref: Long, mmap: SdkMmapRaw) extends io.opentelemetry.api.metrics.DoubleCounter:
  override def add(value: Double): Unit = add(value, Attributes.empty())
  override def add(value: Double, attributes: Attributes): Unit = add(value, attributes, Context.current())
  override def add(value: Double, attributes: Attributes, context: Context): Unit =
    val m = MmapProto.Measurement.newBuilder()
    m.setAsDouble(value)
    m.setMetricRef(metric_ref)
    // TODO - more efficient attribute handling
    attributes.forEach((k,v) => {
      m.addAttributes(AttributeHelper.convertKv(mmap.strings)(k,v))
    })
    m.setSpanContext(internal.convertContext(context))
    // TODO - more efficient clock
    m.setTimeUnixNano(internal.convertInstant(Instant.now()))
    import internal.data.given
    mmap.measurements.write(m.build())

class DoubleHistogramBuilder(metric: MmapProto.MetricRef.Builder, state: MeterSharedState)
extends io.opentelemetry.api.metrics.DoubleHistogramBuilder
// TODO - use this to limit attributes in resulting metric.
with io.opentelemetry.api.incubator.metrics.ExtendedDoubleHistogramBuilder:  
  override def setUnit(unit: String): io.opentelemetry.api.metrics.DoubleHistogramBuilder =
    metric.setUnit(unit)
    this
  override def setDescription(description: String): io.opentelemetry.api.metrics.DoubleHistogramBuilder =
    metric.setDescription(description)
    this
  override def build(): DoubleHistogram =
    val mid = state.mmap.metrics.intern(metric.build())
    DoubleHistogram(mid, state.mmap)
  override def ofLongs(): LongHistogramBuilder = LongHistogramBuilder(metric, state)

class LongHistogramBuilder(metric: MmapProto.MetricRef.Builder, state: MeterSharedState) extends io.opentelemetry.api.metrics.LongHistogramBuilder:  
  override def setUnit(unit: String): io.opentelemetry.api.metrics.LongHistogramBuilder =
    metric.setUnit(unit)
    this
  override def setDescription(description: String): io.opentelemetry.api.metrics.LongHistogramBuilder =
    metric.setDescription(description)
    this
  override def build(): LongHistogram =
    val mid = state.mmap.metrics.intern(metric.build())
    LongHistogram(mid, state.mmap)

class DoubleHistogram(metric_ref: Long, mmap: SdkMmapRaw) extends io.opentelemetry.api.metrics.DoubleHistogram:
  override def record(value: Double): Unit = record(value, Attributes.empty())
  override def record(value: Double, attributes: Attributes): Unit = record(value, attributes, Context.current())
  override def record(value: Double, attributes: Attributes, context: Context): Unit =
    val m = MmapProto.Measurement.newBuilder()
    m.setAsDouble(value)
    m.setMetricRef(metric_ref)
    // TODO - more efficient attribute handling
    attributes.forEach((k,v) => {
      m.addAttributes(AttributeHelper.convertKv(mmap.strings)(k,v))
    })
    m.setSpanContext(internal.convertContext(context))
    // TODO - more efficient clock
    m.setTimeUnixNano(internal.convertInstant(Instant.now()))
    import internal.data.given
    mmap.measurements.write(m.build())

class LongHistogram(metric_ref: Long, mmap: SdkMmapRaw) extends io.opentelemetry.api.metrics.LongHistogram:
  override def record(value: Long): Unit = record(value, Attributes.empty())
  override def record(value: Long, attributes: Attributes): Unit = record(value, attributes, Context.current())
  override def record(value: Long, attributes: Attributes, context: Context): Unit =
    val m = MmapProto.Measurement.newBuilder()
    m.setAsLong(value)
    m.setMetricRef(metric_ref)
    // TODO - more efficient attribute handling
    attributes.forEach((k,v) => {
      m.addAttributes(AttributeHelper.convertKv(mmap.strings)(k,v))
    })
    m.setSpanContext(internal.convertContext(context))
    // TODO - more efficient clock
    m.setTimeUnixNano(internal.convertInstant(Instant.now()))
    import internal.data.given
    mmap.measurements.write(m.build())