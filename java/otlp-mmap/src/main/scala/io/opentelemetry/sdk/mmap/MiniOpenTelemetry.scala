package io.opentelemetry.sdk.mmap

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.api.trace.TracerProvider
import io.opentelemetry.context.propagation.ContextPropagators
import io.opentelemetry.api.metrics.MeterProvider
import io.opentelemetry.api.logs.LoggerProvider


/**
  * An implementation of the OpenTelemetry APIs that tries to push as much as possible into the SDK-MMAP file as possible.
  */
class MiniOpenTelemetry extends OpenTelemetry:
  override def getPropagators(): ContextPropagators = ???
  override def getTracerProvider(): TracerProvider = ???
  override def getMeterProvider(): MeterProvider = ???
  override def getLogsBridge(): LoggerProvider = ???

