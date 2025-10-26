package io.opentelemetry.sdk.mmap

import io.opentelemetry.api.metrics.DoubleGaugeBuilder
import io.opentelemetry.api.metrics.DoubleHistogramBuilder
import io.opentelemetry.api.metrics.LongUpDownCounterBuilder
import io.opentelemetry.api.metrics.LongCounterBuilder

class Meter extends io.opentelemetry.api.metrics.Meter:

  override def counterBuilder(name: String): LongCounterBuilder = ???

  override def upDownCounterBuilder(name: String): LongUpDownCounterBuilder = ???

  override def histogramBuilder(name: String): DoubleHistogramBuilder = ???

  override def gaugeBuilder(name: String): DoubleGaugeBuilder = ???


