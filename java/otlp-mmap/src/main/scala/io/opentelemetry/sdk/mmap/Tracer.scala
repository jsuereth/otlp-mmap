package io.opentelemetry.sdk.mmap

import io.opentelemetry.api.trace.SpanBuilder
import io.opentelemetry.api.trace.SpanContext
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import java.util.concurrent.TimeUnit
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.context.Context
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.StatusCode
import java.time.Instant
import io.opentelemetry.sdk.mmap.internal.SdkMmapRaw
import opentelemetry.proto.mmap.v1.{Mmap=>MmapProto}
import io.opentelemetry.sdk.mmap.internal.data.AttributeHelper
import com.google.protobuf.ByteString
import io.opentelemetry.api.trace.SpanId
import io.opentelemetry.api.trace.TraceFlags
import io.opentelemetry.api.trace.TraceId
import io.opentelemetry.api.trace.TraceState
import io.opentelemetry.sdk.trace.IdGenerator

class TracerProvider(state: TracerProviderSharedState) 
extends io.opentelemetry.api.trace.TracerProvider:
  override def get(instrumentationScopeName: String): io.opentelemetry.api.trace.Tracer = 
    tracerBuilder(instrumentationScopeName).build()
  override def get(instrumentationScopeName: String, instrumentationScopeVersion: String): io.opentelemetry.api.trace.Tracer =
    tracerBuilder(instrumentationScopeName).setInstrumentationVersion(instrumentationScopeVersion).build()

  override def tracerBuilder( instrumentationScopeName: String): io.opentelemetry.api.trace.TracerBuilder =
    TracerBuilder(instrumentationScopeName, state)

case class TracerProviderSharedState(
  resourceId: Long,
  mmap: SdkMmapRaw,
  // TODO - create our own id generator instead of depending on otel SDK.
  id_generator: IdGenerator)

class TracerBuilder(name: String, shared: TracerProviderSharedState) extends io.opentelemetry.api.trace.TracerBuilder:
  private var version = ""
  private var schema_url: String = ""
  override def setInstrumentationVersion(instrumentationScopeVersion: String): io.opentelemetry.api.trace.TracerBuilder =
    version = instrumentationScopeVersion
    this
  override def setSchemaUrl(schemaUrl: String): io.opentelemetry.api.trace.TracerBuilder = 
    schema_url = schemaUrl
    this
  override def build(): io.opentelemetry.api.trace.Tracer =
    Tracer(TracerSharedState(shared.mmap.scopes.intern(shared.resourceId, name, version, schema_url, Attributes.empty()), shared.mmap, shared.id_generator))

case class TracerSharedState(scopeId: Long, mmap: SdkMmapRaw, id_generator: IdGenerator)

class Tracer(state: TracerSharedState) extends io.opentelemetry.api.trace.Tracer:
  override def spanBuilder(spanName: String): SpanBuilder =
    // System.err.println(s"Starting span: ${spanName}")
    val event = MmapProto.SpanEvent.newBuilder()
    event.setScopeRef(state.scopeId)
    event.getStartBuilder.setName(spanName)
    event.getStartBuilder().setKind(MmapProto.SpanEvent.StartSpan.SpanKind.SPAN_KIND_INTERNAL)    
    SpanBuilder(event, state)


class SpanBuilder(event: MmapProto.SpanEvent.Builder, shared: TracerSharedState) extends io.opentelemetry.api.trace.SpanBuilder:
  private var parent = Span.current().getSpanContext()
  override def setNoParent(): SpanBuilder =
    parent = SpanContext.getInvalid()
    this
  override def setStartTimestamp(startTimestamp: Long, unit: TimeUnit): SpanBuilder =
    event.getStartBuilder().setStartTimeUnixNano(unit.toNanos(startTimestamp))
    this
  override def setAttribute[T](key: AttributeKey[T], value: T): SpanBuilder =
    event.getStartBuilder().addAttributes(AttributeHelper.convertKv(shared.mmap.strings)(key,value))
    this
  override def setAttribute(key: String, value: Boolean): SpanBuilder =
    event.getStartBuilder().addAttributes(
      MmapProto.KeyValueRef.newBuilder()
      .setKeyRef(shared.mmap.strings.intern(key))
      .setValue(MmapProto.AnyValue.newBuilder()
      .setBoolValue(value)
      .build())
      .build()
    )
    this
  override def setAttribute(key: String, value: Double): SpanBuilder =
    event.getStartBuilder().addAttributes(
      MmapProto.KeyValueRef.newBuilder()
      .setKeyRef(shared.mmap.strings.intern(key))
      .setValue(MmapProto.AnyValue.newBuilder()
      .setDoubleValue(value)
      .build())
      .build()
    )
    this
  override def setAttribute(key: String, value: Long): SpanBuilder =
    event.getStartBuilder().addAttributes(
      MmapProto.KeyValueRef.newBuilder()
      .setKeyRef(shared.mmap.strings.intern(key))
      .setValue(MmapProto.AnyValue.newBuilder()
      .setIntValue(value)
      .build())
      .build()
    )
    this
  override def setAttribute(key: String, value: String): SpanBuilder =
    event.getStartBuilder().addAttributes(
      MmapProto.KeyValueRef.newBuilder()
      .setKeyRef(shared.mmap.strings.intern(key))
      .setValue(MmapProto.AnyValue.newBuilder()
      .setStringValue(value)
      .build())
      .build()
    )
    this
  override def setParent(context: Context): SpanBuilder = 
    parent = Span.fromContext(context).getSpanContext()
    this

  // TODO - links.  We want these as separate events.
  override def addLink(spanContext: SpanContext, attributes: Attributes): SpanBuilder = ???
  override def addLink(spanContext: SpanContext): SpanBuilder = ???

  override def setSpanKind(spanKind: SpanKind): SpanBuilder =
    spanKind match
      case SpanKind.CLIENT => event.getStartBuilder().setKind(MmapProto.SpanEvent.StartSpan.SpanKind.SPAN_KIND_CLIENT)
      case SpanKind.INTERNAL => event.getStartBuilder().setKind(MmapProto.SpanEvent.StartSpan.SpanKind.SPAN_KIND_INTERNAL)
      case SpanKind.SERVER => event.getStartBuilder().setKind(MmapProto.SpanEvent.StartSpan.SpanKind.SPAN_KIND_SERVER)
      case SpanKind.PRODUCER => event.getStartBuilder().setKind(MmapProto.SpanEvent.StartSpan.SpanKind.SPAN_KIND_PRODUCER)
      case SpanKind.CONSUMER => event.getStartBuilder().setKind(MmapProto.SpanEvent.StartSpan.SpanKind.SPAN_KIND_CONSUMER)    
    this
  override def startSpan(): Span = 
      // TODO - Actually copy sampling logic here.
      val context: SpanContext = 
        if parent.isValid
        then
          // Use parent ids
          event.getStartBuilder().setParentSpanId(ByteString.copyFrom(parent.getSpanIdBytes()))
          SpanContext.create(
            parent.getTraceId(),
            shared.id_generator.generateSpanId(),
            parent.getTraceFlags(),
            parent.getTraceState(),
          )
        else
          // Create our own IDs
          SpanContext.create(
            shared.id_generator.generateTraceId(),
            shared.id_generator.generateSpanId(),
            TraceFlags.getSampled(),
            TraceState.getDefault()
          )
      import internal.data.given
      event.setTraceId(ByteString.copyFrom(context.getTraceIdBytes()))
      event.setSpanId(ByteString.copyFrom(context.getSpanIdBytes()))
      // Check if we have a start time and create one.
      if event.getStart().getStartTimeUnixNano() == 0
      then setStartTimestamp(Instant.now())
      shared.mmap.spans.write(event.build())
      Span(context, shared)

val EXCEPTION_EVENT_NAME = "exception"
val EXCEPTION_TYPE = AttributeKey.stringKey("exception.type")
val EXCEPTION_MESSAGE = AttributeKey.stringKey("exception.message")
val EXCEPTION_STACKTRACE = AttributeKey.stringKey("exception.stacktrace")

class Span(context: SpanContext, shared: TracerSharedState) extends io.opentelemetry.api.trace.Span:
  val status = MmapProto.Status.newBuilder()
  import internal.data.given
  // TODo - optimise this.
  private def newEventWithContext(): MmapProto.SpanEvent.Builder =
    MmapProto.SpanEvent.newBuilder()
    .setScopeRef(shared.scopeId)
    .setTraceId(ByteString.copyFrom(context.getTraceIdBytes()))
    .setSpanId(ByteString.copyFrom(context.getSpanIdBytes()))
  override def updateName(name: String): Span =
    if isRecording
    then
      shared.mmap.spans.write(
        newEventWithContext()
        .setName(MmapProto.SpanEvent.ChangeSpanName.newBuilder()
        .setName(name))
        .build()
      )    
    this
  override def setStatus(statusCode: StatusCode, description: String): Span =
    statusCode match
      case StatusCode.ERROR => status.setCode(MmapProto.Status.StatusCode.STATUS_CODE_ERROR)
      case StatusCode.OK => status.setCode(MmapProto.Status.StatusCode.STATUS_CODE_OK)
      case StatusCode.UNSET => status.setCode(MmapProto.Status.StatusCode.STATUS_CODE_UNSET)    
    status.setMessage(description)
    this
  override def isRecording(): Boolean = context.isSampled() && !context.isRemote()
  override def getSpanContext(): SpanContext = context
  override def setAttribute[T](key: AttributeKey[T], value: T): Span =
    if isRecording
    then
      shared.mmap.spans.write(
        newEventWithContext()
        .setAttributes(
          MmapProto.SpanEvent.UpdateAttributes.newBuilder()
          .addAttributes(
            AttributeHelper.convertKv(shared.mmap.strings)(key, value)
          ))
        .build()
      )    
    this
  override def end(timestamp: Long, unit: TimeUnit): Unit = 
    realEnd(Instant.ofEpochMilli(unit.toMillis(timestamp)))
  override def end(): Unit = realEnd(Instant.now())

  private def realEnd(instant: Instant): Unit =
    if isRecording
    then
      shared.mmap.spans.write(
        newEventWithContext()
        .setEnd(
          MmapProto.SpanEvent.EndSpan.newBuilder()
          .setEndTimeUnixNano(internal.convertInstant(instant))
          .setStatus(status)
          .build()
        )
        .build()
      )
  

  override def recordException(exception: Throwable, additionalAttributes: Attributes): Span = 
    val exceptionName = exception.getClass().getCanonicalName()
    val exceptionMessage = exception.getMessage()
    val stringWriter = new java.io.StringWriter()
    scala.util.Using(new java.io.PrintWriter(stringWriter))(exception.printStackTrace)
    val stackTrace = stringWriter.toString();
    val attributes = additionalAttributes.toBuilder()
    if (exceptionName != null) {
      attributes.put(EXCEPTION_TYPE, exceptionName);
    }
    if (exceptionMessage != null) {
      attributes.put(EXCEPTION_MESSAGE, exceptionMessage);
    }
    if (stackTrace != null) {
      attributes.put(EXCEPTION_STACKTRACE, stackTrace);
    }
    addEvent(EXCEPTION_EVENT_NAME, attributes.build())
    this
  override def addEvent(name: String, attributes: Attributes): Span =
    addEventNow(name, attributes, Instant.now())
  override def addEvent(name: String, attributes: Attributes, timestamp: Long, unit: TimeUnit): Span =
    addEventNow(name, attributes, Instant.ofEpochMilli(unit.toMillis(timestamp)))
  private def addEventNow(name: String, attributes: Attributes, timestamp: Instant): Span =
    if isRecording()
    then
      val event = MmapProto.Event.newBuilder()
      event.setSpanContext(internal.convertSpanContext(context))
      event.setScopeRef(shared.scopeId)
      event.setTimeUnixNano(internal.convertInstant(timestamp))
      event.setEventNameRef(shared.mmap.strings.intern(name))
      // TODO - Where should we store attributes?
      attributes.forEach((k,v) => {
        event.addAttributes(AttributeHelper.convertKv(shared.mmap.strings)(k,v))
      })
      shared.mmap.events.write(event.build())
    this
  