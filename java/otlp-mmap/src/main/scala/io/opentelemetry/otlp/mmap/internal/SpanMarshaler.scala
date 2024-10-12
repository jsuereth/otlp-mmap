package io.opentelemetry.otlp.mmap.internal

import io.opentelemetry.exporter.internal.otlp.traces.ResourceSpansMarshaler
import io.opentelemetry.exporter.internal.marshal.MarshalerWithSize
import io.opentelemetry.exporter.internal.marshal.MarshalerUtil
import io.opentelemetry.sdk.trace.data.SpanData
import io.opentelemetry.proto.trace.v1.internal.Span
import io.opentelemetry.exporter.internal.otlp.traces.SpanFlags
import io.opentelemetry.exporter.internal.marshal.Serializer
import io.opentelemetry.exporter.internal.otlp.KeyValueMarshaler
import io.opentelemetry.api.trace.propagation.internal.W3CTraceContextEncoding
import java.nio.charset.StandardCharsets
import io.opentelemetry.exporter.internal.marshal.ProtoEnumInfo
import io.opentelemetry.proto.trace.v1.internal.Status
import io.opentelemetry.sdk.trace.data.StatusData
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.TraceFlags
import io.opentelemetry.exporter.internal.marshal.Marshaler
import io.opentelemetry.exporter.internal.marshal.ProtoFieldInfo

// Our newly created proto where we shovel spans with references to serialzied resource/scope
// dictionaries.
object ResourceRefSpanMarshaler:
    def apply(s: SpanData, resourceId: Long, scopeId: Long): ResourceRefSpanMarshaler =
        val spanMarshaler = SpanMarshaler(s)
        new ResourceRefSpanMarshaler(spanMarshaler, resourceId, scopeId)

    private def calculateSize(span: SpanMarshaler, resourceId: Long, scopeId: Long): Int =
        var size = 0
        // TODO - probably want these to be int32s
        size += MarshalerUtil.sizeInt64(RESOURCE_REF, resourceId)
        size += MarshalerUtil.sizeInt64(SCOPE_REF, scopeId)
        size += MarshalerUtil.sizeMessage(SPAN, span)
        size
    val RESOURCE_REF = ProtoFieldInfo.create(1, 8, "resource_ref")
    val SCOPE_REF = ProtoFieldInfo.create(2, 16, "scope_ref")
    val SPAN = ProtoFieldInfo.create(3, 26, "span")
    

class ResourceRefSpanMarshaler private(span: SpanMarshaler, resourceId: Long, scopeId: Long)
extends MarshalerWithSize(ResourceRefSpanMarshaler.calculateSize(span, resourceId, scopeId)):
  override def writeTo(output: Serializer): Unit =
    output.serializeInt64(ResourceRefSpanMarshaler.RESOURCE_REF, resourceId)
    output.serializeInt64(ResourceRefSpanMarshaler.SCOPE_REF, scopeId)
    output.serializeMessage(ResourceRefSpanMarshaler.SPAN, span)

// NOTE: All of this code is copied from OTEL Java's internal otlp helpers, because they are hidden
// from being reused.


class SpanMarshaler private(traceId: String,
      spanId: String,
      traceStateUtf8: Array[Byte],
      parentSpanId: String,
      nameUtf8: Array[Byte],
      spanKind: ProtoEnumInfo,
      startEpochNanos: Long,
      endEpochNanos: Long,
      attributeMarshalers: Array[KeyValueMarshaler],
      droppedAttributesCount: Int,
      spanEventMarshalers: Array[MarshalerWithSize],
      droppedEventsCount: Int,
      spanLinkMarshalers: Array[MarshalerWithSize],
      droppedLinksCount: Int,
      spanStatusMarshaler: SpanStatusMarshaler,
      flags: TraceFlags,
      isParentContextRemote: Boolean) extends MarshalerWithSize(SpanMarshaler.calculateSpanSize(traceId,
            spanId,
            traceStateUtf8,
            parentSpanId,
            nameUtf8,
            spanKind,
            startEpochNanos,
            endEpochNanos,
            attributeMarshalers,
            droppedAttributesCount,
            spanEventMarshalers,
            droppedEventsCount,
            spanLinkMarshalers,
            droppedLinksCount,
            spanStatusMarshaler,
            flags,
            isParentContextRemote)):    
    override def writeTo(output: Serializer): Unit =
        output.serializeTraceId(Span.TRACE_ID, traceId)
        output.serializeSpanId(Span.SPAN_ID, spanId)
        output.serializeString(Span.TRACE_STATE, traceStateUtf8)
        output.serializeSpanId(Span.PARENT_SPAN_ID, parentSpanId)
        output.serializeString(Span.NAME, nameUtf8)
        output.serializeEnum(Span.KIND, spanKind)
        output.serializeFixed64(Span.START_TIME_UNIX_NANO, startEpochNanos)
        output.serializeFixed64(Span.END_TIME_UNIX_NANO, endEpochNanos)
        output.serializeRepeatedMessage(Span.ATTRIBUTES, attributeMarshalers.asInstanceOf[Array[Marshaler]])
        output.serializeUInt32(Span.DROPPED_ATTRIBUTES_COUNT, droppedAttributesCount)
        output.serializeRepeatedMessage(Span.EVENTS, spanEventMarshalers.asInstanceOf[Array[Marshaler]])
        output.serializeUInt32(Span.DROPPED_EVENTS_COUNT, droppedEventsCount)
        output.serializeRepeatedMessage(Span.LINKS, spanLinkMarshalers.asInstanceOf[Array[Marshaler]])
        output.serializeUInt32(Span.DROPPED_LINKS_COUNT, droppedLinksCount)
        output.serializeMessage(Span.STATUS, spanStatusMarshaler)
        output.serializeFixed32(
            Span.FLAGS, SpanFlags.withParentIsRemoteFlags(flags, isParentContextRemote))

object SpanMarshaler:
    def apply(s: SpanData): SpanMarshaler =
        val attributeMarshalers = KeyValueMarshaler.createForAttributes(s.getAttributes())
        val spanEventMarshalers = Array[MarshalerWithSize]() // TODO
        val spanLinkMarshalers = Array[MarshalerWithSize]() // TODO
        val parentSpanId =
            if s.getParentSpanContext().isValid()
            then s.getParentSpanContext().getSpanId()
            else null
        val traceStateUtf8 = encodeTraceState(s)
        new SpanMarshaler(
            s.getSpanContext().getTraceId(),
            s.getSpanContext().getSpanId(),
            traceStateUtf8,
            parentSpanId,
            MarshalerUtil.toBytes(s.getName()),
            toProtoSpanKind(s.getKind()),
            s.getStartEpochNanos(),
            s.getEndEpochNanos(),
            attributeMarshalers,
            s.getTotalAttributeCount() - s.getAttributes().size(),
            spanEventMarshalers,
            s.getTotalRecordedEvents() - s.getEvents().size(),
            spanLinkMarshalers,
            s.getTotalRecordedLinks() - s.getLinks().size(),
            SpanStatusMarshaler(s.getStatus()),
            s.getSpanContext().getTraceFlags(),
            s.getParentSpanContext().isRemote())

    val EMPTY_BYTES=Array[Byte]()

    private def calculateSpanSize(traceId: String,
      spanId: String,
      traceStateUtf8: Array[Byte],
      parentSpanId: String,
      nameUtf8: Array[Byte],
      spanKind: ProtoEnumInfo,
      startEpochNanos: Long,
      endEpochNanos: Long,
      attributeMarshalers: Array[KeyValueMarshaler],
      droppedAttributesCount: Int,
      spanEventMarshalers: Array[MarshalerWithSize],
      droppedEventsCount: Int,
      spanLinkMarshalers: Array[MarshalerWithSize],
      droppedLinksCount: Int,
      spanStatusMarshaler: SpanStatusMarshaler,
      flags: TraceFlags,
      isParentContextRemote: Boolean): Int =
        var size = 0
        size += MarshalerUtil.sizeTraceId(Span.TRACE_ID, traceId)
        size += MarshalerUtil.sizeSpanId(Span.SPAN_ID, spanId)
        size += MarshalerUtil.sizeBytes(Span.TRACE_STATE, traceStateUtf8)
        size += MarshalerUtil.sizeSpanId(Span.PARENT_SPAN_ID, parentSpanId)
        size += MarshalerUtil.sizeBytes(Span.NAME, nameUtf8);
        size += MarshalerUtil.sizeEnum(Span.KIND, spanKind)
        size += MarshalerUtil.sizeFixed64(Span.START_TIME_UNIX_NANO, startEpochNanos)
        size += MarshalerUtil.sizeFixed64(Span.END_TIME_UNIX_NANO, endEpochNanos)
        size += MarshalerUtil.sizeRepeatedMessage(Span.ATTRIBUTES, attributeMarshalers)
        size += MarshalerUtil.sizeUInt32(Span.DROPPED_ATTRIBUTES_COUNT, droppedAttributesCount)
        size += MarshalerUtil.sizeRepeatedMessage(Span.EVENTS, spanEventMarshalers)
        size += MarshalerUtil.sizeUInt32(Span.DROPPED_EVENTS_COUNT, droppedEventsCount)
        size += MarshalerUtil.sizeRepeatedMessage(Span.LINKS, spanLinkMarshalers)
        size += MarshalerUtil.sizeUInt32(Span.DROPPED_LINKS_COUNT, droppedLinksCount)
        size += MarshalerUtil.sizeMessage(Span.STATUS, spanStatusMarshaler)
        size += MarshalerUtil.sizeFixed32(Span.FLAGS, SpanFlags.withParentIsRemoteFlags(flags, isParentContextRemote))
        size

    private def encodeTraceState(s: SpanData): Array[Byte] =
        val traceState = s.getSpanContext().getTraceState()
        if traceState.isEmpty()
        then EMPTY_BYTES
        else W3CTraceContextEncoding.encodeTraceState(traceState).getBytes(StandardCharsets.UTF_8)
    private def toProtoSpanKind(k: SpanKind): ProtoEnumInfo =
        k match
            case SpanKind.INTERNAL => Span.SpanKind.SPAN_KIND_INTERNAL
            case SpanKind.SERVER => Span.SpanKind.SPAN_KIND_SERVER
            case SpanKind.CLIENT => Span.SpanKind.SPAN_KIND_CLIENT
            case SpanKind.PRODUCER => Span.SpanKind.SPAN_KIND_PRODUCER
            case SpanKind.CONSUMER => Span.SpanKind.SPAN_KIND_CONSUMER
            case null => Span.SpanKind.SPAN_KIND_UNSPECIFIED


/** Helper to serialzie proto messages of span status. */
class SpanStatusMarshaler(statusCode: ProtoEnumInfo, descriptionUtf8: Array[Byte]) extends MarshalerWithSize(SpanStatusMarshaler.computeSize(statusCode, descriptionUtf8)):
    override def writeTo(output: Serializer): Unit =
        output.serializeString(Status.MESSAGE, descriptionUtf8)
        output.serializeEnum(Status.CODE, statusCode)

object SpanStatusMarshaler:
    def apply(s: StatusData): SpanStatusMarshaler =
        val statusCode = toProtoStatusCode(s)
        val descriptionUtf8 = MarshalerUtil.toBytes(s.getDescription())
        new SpanStatusMarshaler(statusCode, descriptionUtf8)
    private def computeSize(statusCode: ProtoEnumInfo, descriptionUtf8: Array[Byte]): Int =
        var size = 0
        size = size + MarshalerUtil.sizeBytes(Status.MESSAGE, descriptionUtf8)
        size = size + MarshalerUtil.sizeEnum(Status.CODE, statusCode)
        size
    private def toProtoStatusCode(s: StatusData): ProtoEnumInfo =
        s.getStatusCode() match 
          case StatusCode.OK => Status.StatusCode.STATUS_CODE_OK
          case StatusCode.ERROR => Status.StatusCode.STATUS_CODE_ERROR
          case _ => Status.StatusCode.STATUS_CODE_UNSET