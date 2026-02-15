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
import io.opentelemetry.api.metrics.LongGaugeBuilder
import io.opentelemetry.api.metrics.ObservableDoubleGauge
import io.opentelemetry.api.metrics.ObservableLongGauge
import io.opentelemetry.api.metrics.ObservableLongUpDownCounter
import io.opentelemetry.api.metrics.ObservableDoubleUpDownCounter
import java.util.Timer
import java.util.TimerTask
import java.util.concurrent.TimeUnit


class MeterProvider(state: MeterProviderState) extends io.opentelemetry.api.metrics.MeterProvider:
  override def meterBuilder(instrumentationScopeName: String): MeterBuilder =
    println(s"Building meter: ${instrumentationScopeName}")
    MeterBuilder(instrumentationScopeName, state)

case class MeterProviderState(
  resource_ref: Long,
  mmap: SdkMmapRaw,
  timer: Timer)

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
    val meter_state = MeterSharedState(sid, provider_state.mmap, java.util.ArrayList())
    // TODO - Configuration for this.
    val jitter = TimeUnit.SECONDS.toMillis(10)
    val rate = TimeUnit.SECONDS.toMillis(30)
    provider_state.timer.scheduleAtFixedRate(meter_state.collectionTask, jitter, rate)
    Meter(meter_state)

case class MeterSharedState(
  scopeId: Long,
  mmap: SdkMmapRaw,
  async_instruments: java.util.ArrayList[RegisteredMetric]
):
  def register(m: RegisteredMetric): Unit =
    async_instruments.synchronized {
      async_instruments.add(m)
    }
  val collectionTask = new TimerTask:
    override def run(): Unit =
      async_instruments.synchronized {
        async_instruments.forEach(_.collect())
      }


trait RegisteredMetric:
  def collect(): Unit
class RegisteredAsyncLongMetric(mmap: SdkMmapRaw, metric_ref: Long, callback: Consumer[ObservableLongMeasurement])
extends RegisteredMetric
with ObservableLongMeasurement:
  override def record(value: Long): Unit = record(value, Attributes.empty())
  override def record(value: Long, attributes: Attributes): Unit =
    val m = MmapProto.Measurement.newBuilder()
    m.setAsLong(value)
    m.setMetricRef(metric_ref)
    // TODO - more efficient attribute handling
    attributes.forEach((k,v) => {
      m.addAttributes(AttributeHelper.convertKv(mmap.strings)(k,v))
    })
    // TODO - more efficient clock
    m.setTimeUnixNano(internal.convertInstant(Instant.now()))
    import internal.data.given
    mmap.measurements.write(m.build())
  override def collect(): Unit = callback.accept(this)
class RegisteredAsyncDoubleMetric(mmap: SdkMmapRaw, metric_ref: Long, callback: Consumer[ObservableDoubleMeasurement]) 
extends RegisteredMetric
with ObservableDoubleMeasurement:
  override def record(value: Double): Unit = record(value, Attributes.empty())
  override def record(value: Double, attributes: Attributes): Unit =
    val m = MmapProto.Measurement.newBuilder()
    m.setAsDouble(value)
    m.setMetricRef(metric_ref)
    // TODO - more efficient attribute handling
    attributes.forEach((k,v) => {
      m.addAttributes(AttributeHelper.convertKv(mmap.strings)(k,v))
    })
    // TODO - more efficient clock
    m.setTimeUnixNano(internal.convertInstant(Instant.now()))
    import internal.data.given
    mmap.measurements.write(m.build())
  override def collect(): Unit = callback.accept(this)


/// Implementation of meter that records metric definitions in a dictionary, and measurements in ringbuffer.
class Meter(state: MeterSharedState) extends io.opentelemetry.api.metrics.Meter:

  override def counterBuilder(name: String): LongCounterBuilder = 
    println(s"Building counter: ${name}")
    val m = MmapProto.MetricRef.newBuilder()
    m.setName(name)
    m.setInstrumentationScopeRef(state.scopeId)
    m.setSum(MmapProto.MetricRef.Sum.newBuilder()
    // TODO - pull temporality from SDK view config?
    .setAggregationTemporality(MmapProto.AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE)
    .setIsMonotonic(true)
    .build())
    LongCounterBuilder(state, m)

  override def upDownCounterBuilder(name: String): LongUpDownCounterBuilder =
    val m = MmapProto.MetricRef.newBuilder()
    m.setName(name)
    m.setInstrumentationScopeRef(state.scopeId)
    m.setSum(
      MmapProto.MetricRef.Sum.newBuilder()
      .setAggregationTemporality(MmapProto.AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE)
      .setIsMonotonic(false)
      .build()
    )
    LongUpDownCounterBuilder(state, m)

  override def histogramBuilder(name: String): DoubleHistogramBuilder =
    val m = MmapProto.MetricRef.newBuilder()
    m.setName(name)
    m.setInstrumentationScopeRef(state.scopeId)
    m.setExpHist(MmapProto.MetricRef.ExponentialHistogram.newBuilder()
    .setAggregationTemporality(MmapProto.AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE)
    .setMaxScale(20)
    .setMaxBuckets(20)
    )
    DoubleHistogramBuilder(m, state)

  override def gaugeBuilder(name: String): DoubleGaugeBuilder =
    val m = MmapProto.MetricRef.newBuilder()
    m.setName(name)
    m.setInstrumentationScopeRef(state.scopeId)
    m.setGauge(MmapProto.MetricRef.Gauge.newBuilder().build())
    DoubleGaugeBuilder(state, m)


class DoubleGaugeBuilder(state: MeterSharedState, metric: MmapProto.MetricRef.Builder) extends io.opentelemetry.api.metrics.DoubleGaugeBuilder:

  override def buildWithCallback(callback: Consumer[ObservableDoubleMeasurement]): ObservableDoubleGauge =
    val r = RegisteredAsyncDoubleMetric(state.mmap, state.mmap.metrics.intern(metric.build()), callback)
    state.register(r)
    new ObservableDoubleGauge {}

  override def ofLongs(): LongGaugeBuilder = LongGaugeBuilder(state, metric)

  override def setUnit(unit: String): io.opentelemetry.api.metrics.DoubleGaugeBuilder =
    metric.setUnit(unit)
    this

  override def setDescription(description: String): io.opentelemetry.api.metrics.DoubleGaugeBuilder =
    metric.setDescription(description)
    this

class LongGaugeBuilder(state: MeterSharedState, metric: MmapProto.MetricRef.Builder) extends io.opentelemetry.api.metrics.LongGaugeBuilder:

  override def buildWithCallback(callback: Consumer[ObservableLongMeasurement]): io.opentelemetry.api.metrics.ObservableLongGauge =
    val r = RegisteredAsyncLongMetric(state.mmap, state.mmap.metrics.intern(metric.build()), callback)
    state.register(r)
    new ObservableLongGauge {}

  override def setUnit(unit: String): io.opentelemetry.api.metrics.LongGaugeBuilder =
    metric.setUnit(unit)
    this

  override def setDescription(description: String): io.opentelemetry.api.metrics.LongGaugeBuilder =
    metric.setDescription(description)
    this

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
    val r = RegisteredAsyncLongMetric(state.mmap, state.mmap.metrics.intern(metric.build()), callback)
    state.register(r)
    new ObservableLongCounter {}


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
    val r = RegisteredAsyncDoubleMetric(state.mmap, state.mmap.metrics.intern(metric.build()), callback)
    state.register(r)
    new ObservableDoubleCounter {}

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



class LongUpDownCounterBuilder(state: MeterSharedState, metric: opentelemetry.proto.mmap.v1.Mmap.MetricRef.Builder) extends io.opentelemetry.api.metrics.LongUpDownCounterBuilder:
  override def setUnit(unit: String): io.opentelemetry.api.metrics.LongUpDownCounterBuilder =
    metric.setUnit(unit)
    this
  override def setDescription(description: String): io.opentelemetry.api.metrics.LongUpDownCounterBuilder =
      metric.setDescription(description)
      this

  override def ofDoubles(): DoubleUpDownCounterBuilder =
    DoubleUpDownCounterBuilder(state, metric)

  override def build(): LongUpDownCounter = 
    // TODO - memoize the metric
    val mid = state.mmap.metrics.intern(metric.build())
    LongUpDownCounter(mid, state.mmap)

  override def buildWithCallback(callback: Consumer[ObservableLongMeasurement]): io.opentelemetry.api.metrics.ObservableLongUpDownCounter = 
    val r = RegisteredAsyncLongMetric(state.mmap, state.mmap.metrics.intern(metric.build()), callback)
    state.register(r)
    new ObservableLongUpDownCounter {}

class DoubleUpDownCounterBuilder(state: MeterSharedState, metric: opentelemetry.proto.mmap.v1.Mmap.MetricRef.Builder) extends io.opentelemetry.api.metrics.DoubleUpDownCounterBuilder:
  override def setUnit(unit: String): io.opentelemetry.api.metrics.DoubleUpDownCounterBuilder =
    metric.setUnit(unit)
    this
  override def setDescription(description: String): io.opentelemetry.api.metrics.DoubleUpDownCounterBuilder =
      metric.setDescription(description)
      this

  override def build(): DoubleUpDownCounter = 
    // TODO - memoize the metric
    val mid = state.mmap.metrics.intern(metric.build())
    DoubleUpDownCounter(mid, state.mmap)

  override def buildWithCallback(callback: Consumer[ObservableDoubleMeasurement]): io.opentelemetry.api.metrics.ObservableDoubleUpDownCounter = 
    val r = RegisteredAsyncDoubleMetric(state.mmap, state.mmap.metrics.intern(metric.build()), callback)
    state.register(r)
    new ObservableDoubleUpDownCounter {}

class LongUpDownCounter(metric_ref: Long, mmap: SdkMmapRaw) extends io.opentelemetry.api.metrics.LongUpDownCounter:
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

class DoubleUpDownCounter(metric_ref: Long, mmap: SdkMmapRaw) extends io.opentelemetry.api.metrics.DoubleUpDownCounter:
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