package io.opentelemetry.sdk.mmap

import io.opentelemetry.api.logs.LogRecordBuilder
import java.util.concurrent.TimeUnit
import io.opentelemetry.api.logs.Severity
import io.opentelemetry.context.Context
import io.opentelemetry.api.common.AttributeKey
import java.time.Instant
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.sdk.mmap.internal.ByteBufferOutputStream
import io.opentelemetry.api.logs.LoggerBuilder
import io.opentelemetry.sdk.mmap.internal.Dictionary
import io.opentelemetry.sdk.mmap.internal.data.ScopeDictionary
import io.opentelemetry.api.trace.SpanContext
import com.google.protobuf.ByteString
import io.opentelemetry.sdk.mmap.internal.SdkMmapRaw
import io.opentelemetry.sdk.mmap.internal.data.AttributeHelper

/** Implementation of logger provider that fires all events on ringbuffer, and memoizes scope to dictionary. */
class LoggerProvider(state: LoggerProviderSharedState) extends io.opentelemetry.api.logs.LoggerProvider:
  override def loggerBuilder(instrumentationScopeName: String): LoggerBuilder = 
    LoggerBuilder(instrumentationScopeName, state)

/** Shared state for logger provider implementation */
case class LoggerProviderSharedState(resource_ref: Long, mmap: SdkMmapRaw)

/** Our implementation of a LoggerBuilder that will memoize information to the dictionary. */
class LoggerBuilder(name: String, provider_state: LoggerProviderSharedState) extends io.opentelemetry.api.logs.LoggerBuilder:
  private var version = ""
  private var schema_url = ""
  private val attributes = Attributes.builder()

  override def build(): io.opentelemetry.api.logs.Logger =
    val scope_id = provider_state.mmap.scopes.intern(provider_state.resource_ref, name, version, schema_url, attributes.build())
    val state = LoggerSharedState(scope_id, provider_state.mmap)
    Logger(state)

  override def setInstrumentationVersion(instrumentationScopeVersion: String): io.opentelemetry.api.logs.LoggerBuilder =
    this.version = instrumentationScopeVersion
    this

  override def setSchemaUrl(schemaUrl: String): io.opentelemetry.api.logs.LoggerBuilder =
    this.schema_url = schemaUrl
    this

/** Our implementaiton of a logger which fire protos to the ring buffer. */
class Logger(state: LoggerSharedState) extends io.opentelemetry.api.logs.Logger: 
  override def logRecordBuilder(): LogRecordBuilder = LogRecordBuilder(state)


case class LoggerSharedState(scopeId: Long, mmap: SdkMmapRaw)

class LogRecordBuilder(loggerState: LoggerSharedState) extends io.opentelemetry.api.logs.LogRecordBuilder:
  val event = opentelemetry.proto.mmap.v1.Mmap.Event.newBuilder()
  // TODO - Event name?
  {
    val now = Instant.now()
    setContext(Context.current())
    setObservedTimestamp(now)
    setTimestamp(now)
    setSeverity(Severity.DEBUG)
    event.setScopeRef(loggerState.scopeId)
  }

  override def setSeverity(severity: Severity): LogRecordBuilder =
    event.setSeverityNumberValue(severity.getSeverityNumber())
    this
  override def setAttribute[T](key: AttributeKey[T], value: T): LogRecordBuilder =
    val ref = AttributeHelper.convertKv(loggerState.mmap.strings)(key,value)
    event.addAttributes(ref)
    this
  override def setSeverityText(severityText: String): LogRecordBuilder =
    event.setSeverityText(severityText)
    this
  override def setObservedTimestamp(instant: Instant): LogRecordBuilder = 
    // TODO - add this to protocol?
    this
  override def setObservedTimestamp(timestamp: Long, unit: TimeUnit): LogRecordBuilder =
    setObservedTimestamp(Instant.ofEpochMilli(unit.toMillis(timestamp)))
  override def setBody(body: String): LogRecordBuilder = 
    event.setBody(opentelemetry.proto.mmap.v1.Mmap.AnyValue.newBuilder().setStringValue(body).build())
    this
  override def setTimestamp(instant: Instant): LogRecordBuilder =
    event.setTimeUnixNano(internal.convertInstant(instant))
    this
  override def setTimestamp(timestamp: Long, unit: TimeUnit): LogRecordBuilder =
    setTimestamp(Instant.ofEpochMilli(unit.toMillis(timestamp)))
  override def setContext(context: Context): LogRecordBuilder =
    event.setSpanContext(internal.convertContext(context))
    this
  override def setEventName(eventName: String): io.opentelemetry.api.logs.LogRecordBuilder =
    event.setEventNameRef(loggerState.mmap.strings.intern(eventName))
    this
  override def emit(): Unit = 
      // Send out output channel.
      // TODO - shrink size if needed.
      loggerState.mmap.events.writeToNextBuffer(buf => event.build().writeDelimitedTo(new ByteBufferOutputStream(buf)))

