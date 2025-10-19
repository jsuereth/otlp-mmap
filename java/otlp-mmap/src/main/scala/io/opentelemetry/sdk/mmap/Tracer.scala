package io.opentelemetry.sdk.mmap

import io.opentelemetry.api.trace.SpanBuilder
import io.opentelemetry.api.trace.SpanContext
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import java.util.concurrent.TimeUnit
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.context.Context
import io.opentelemetry.api.trace.Span
import io.opentelemetry.otlp.mmap.internal.RingBufferOutputChannel
import io.opentelemetry.api.trace.StatusCode
import java.time.Instant

class Tracer extends io.opentelemetry.api.trace.Tracer:
  override def spanBuilder(spanName: String): SpanBuilder = ???  


class TracerSharedState(resourceId: Int, scopeId: Int, spanStartOut: RingBufferOutputChannel)

class SpanBuilder(traceState: TracerSharedState) extends io.opentelemetry.api.trace.SpanBuilder:
  private var parent = Span.current().getSpanContext()
  private var kind = SpanKind.INTERNAL
  private var attributes = Attributes.builder()
  private var startEpochNanos = 0L;
  override def setNoParent(): SpanBuilder =
    parent = SpanContext.getInvalid()
    this
  override def setStartTimestamp(startTimestamp: Long, unit: TimeUnit): SpanBuilder =
    startEpochNanos = unit.toNanos(startTimestamp)
    this
  override def setAttribute[T](key: AttributeKey[T], value: T): SpanBuilder =
    attributes.put(key, value)
    this
  override def setAttribute(key: String, value: Boolean): SpanBuilder =
    attributes.put(key, value)
    this
  override def setAttribute(key: String, value: Double): SpanBuilder =
    attributes.put(key, value)
    this
  override def setAttribute(key: String, value: Long): SpanBuilder =
    attributes.put(key, value)
    this
  override def setAttribute(key: String, value: String): SpanBuilder =
    attributes.put(key, value)
    this
  override def setParent(context: Context): SpanBuilder = 
    parent = Span.fromContext(context).getSpanContext()
    this

  // TODO - links.
  override def addLink(spanContext: SpanContext, attributes: Attributes): SpanBuilder = ???
  override def addLink(spanContext: SpanContext): SpanBuilder = ???

  override def setSpanKind(spanKind: SpanKind): SpanBuilder =
    kind = spanKind
    this
  override def startSpan(): Span = 
      // TODO - find trace/span ids
      // TODO - sample
      // TODO - write span out.
      ???

val EXCEPTION_EVENT_NAME = "exception"
val EXCEPTION_TYPE = AttributeKey.stringKey("exception.type")
val EXCEPTION_MESSAGE = AttributeKey.stringKey("exception.message")
val EXCEPTION_STACKTRACE = AttributeKey.stringKey("exception.stacktrace")

class Span(context: SpanContext, traceState: TracerSharedState) extends io.opentelemetry.api.trace.Span:
  private var nameUpdate: Option[String] = None
  private var attributes = Attributes.builder()
  override def updateName(name: String): Span =
    nameUpdate = Some(name)
    this
  override def setStatus(statusCode: StatusCode, description: String): Span = ???
  override def isRecording(): Boolean = context.isSampled() && !context.isRemote()
  override def getSpanContext(): SpanContext = context
  override def setAttribute[T](key: AttributeKey[T], value: T): Span =
    attributes.put(key, value)
    this
  override def end(timestamp: Long, unit: TimeUnit): Unit = realEnd(Instant.ofEpochMilli(unit.toMillis(timestamp)))
  override def end(): Unit = realEnd(Instant.now())

  private def realEnd(instant: Instant): Unit =
    // TODO - Emit SpenEnd event.
    ()
  

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
    /// TODO - write to event channel.
    ???
  