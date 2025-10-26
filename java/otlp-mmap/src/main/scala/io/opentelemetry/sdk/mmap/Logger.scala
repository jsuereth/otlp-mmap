package io.opentelemetry.sdk.mmap

import io.opentelemetry.api.logs.LogRecordBuilder
import java.util.concurrent.TimeUnit
import io.opentelemetry.api.logs.Severity
import io.opentelemetry.context.Context
import io.opentelemetry.api.common.AttributeKey
import java.time.Instant
import io.opentelemetry.otlp.mmap.internal.RingBufferOutputChannel
import io.opentelemetry.api.common.Attributes

class Logger extends io.opentelemetry.api.logs.Logger: 

  override def logRecordBuilder(): LogRecordBuilder = ???


class LoggerSharedState(resourceId: Int, scopeId: Int, out: RingBufferOutputChannel)

class LogRecordBuilder(loggerState: LoggerSharedState) extends io.opentelemetry.api.logs.LogRecordBuilder:
  var timestamp = Instant.now()
  var observedTimestamp = timestamp
  var context = Context.current()
  var severity = Severity.DEBUG
  var severityText: Option[String] = None
  var attributes = Attributes.builder()
  

  override def setSeverity(severity: Severity): LogRecordBuilder =
    this.severity = severity
    this
  override def setAttribute[T](key: AttributeKey[T], value: T): LogRecordBuilder =
    attributes.put(key, value)
    this
  override def setSeverityText(severityText: String): LogRecordBuilder =
    this.severityText = Some(severityText)
    this
  override def setObservedTimestamp(instant: Instant): LogRecordBuilder = 
    observedTimestamp = instant
    this
  override def setObservedTimestamp(timestamp: Long, unit: TimeUnit): LogRecordBuilder =
    setObservedTimestamp(Instant.ofEpochMilli(unit.toMillis(timestamp)))
  override def setBody(body: String): LogRecordBuilder = ???
  override def setTimestamp(instant: Instant): LogRecordBuilder =
    timestamp = instant
    this
  override def setTimestamp(timestamp: Long, unit: TimeUnit): LogRecordBuilder =
    setTimestamp(Instant.ofEpochMilli(unit.toMillis(timestamp)))
  override def setContext(context: Context): LogRecordBuilder =
    this.context = context
    this
  override def emit(): Unit = 
      // Send out output channel.
      ???

