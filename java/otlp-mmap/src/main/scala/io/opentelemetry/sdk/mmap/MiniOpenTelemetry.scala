package io.opentelemetry.sdk.mmap

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.context.propagation.ContextPropagators
import io.opentelemetry.sdk.mmap.internal.SdkMmapRaw
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.context.propagation.TextMapPropagator
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.api.baggage.propagation.W3CBaggagePropagator
import io.opentelemetry.sdk.trace.IdGenerator
import java.util.Timer


/**
  * An implementation of the OpenTelemetry APIs that tries to push as much as possible into the SDK-MMAP file as possible.
  */
class MiniOpenTelemetry(mmap: SdkMmapRaw) extends OpenTelemetry:
  // TODO - Don't use OpenTelemetry SDK for this.
  private val resource_ref = mmap.resources.intern(Resource.getDefault())
  // TODO - View config, Async Instrument Config.
  private val meters = MeterProvider(MeterProviderState(resource_ref, mmap, new Timer))
  private val logs = LoggerProvider(LoggerProviderSharedState(resource_ref, mmap))
  // TODO - Sampling config
  private val spans = TracerProvider(TracerProviderSharedState(resource_ref, mmap, IdGenerator.random()))
  private val propagators = ContextPropagators.create(
    TextMapPropagator.composite(
      W3CTraceContextPropagator.getInstance(),
      W3CBaggagePropagator.getInstance()
    )
  )
  override def getPropagators(): ContextPropagators = propagators
  override def getTracerProvider(): io.opentelemetry.api.trace.TracerProvider = spans
  override def getMeterProvider(): io.opentelemetry.api.metrics.MeterProvider = meters
  override def getLogsBridge(): io.opentelemetry.api.logs.LoggerProvider = logs

