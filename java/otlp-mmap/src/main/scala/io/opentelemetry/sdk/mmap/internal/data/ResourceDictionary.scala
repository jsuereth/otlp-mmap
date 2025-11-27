package io.opentelemetry.sdk.mmap.internal
package data

import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.exporter.internal.marshal.MarshalerWithSize
import io.opentelemetry.exporter.internal.marshal.Serializer
import io.opentelemetry.exporter.internal.marshal.Marshaler
import io.opentelemetry.exporter.internal.marshal.MarshalerUtil
import java.nio.ByteBuffer
import io.opentelemetry.sdk.mmap.internal.ByteBufferOutputStream
import io.opentelemetry.api.common.AttributeType


given Writable[MarshalerWithSize] with
  extension (data: MarshalerWithSize) override def write(buffer: ByteBuffer): Unit =
    data.writeBinaryTo(new ByteBufferOutputStream(buffer))
  extension (data: MarshalerWithSize) override def size: Long =
    data.getBinarySerializedSize()



/** Stores opentelemetry Resource in the OTLP mmap dictionary. */
class ResourceDictionary(d: Dictionary, strings: StringDictionary):
    // TODO - We'll want some kind of limit to avoid OOM-ing.
    // We also need to benchmark memory overhead of this *specific* region if possible.
    private val memos = java.util.concurrent.ConcurrentHashMap[Resource, Long]
    def intern(r: Resource): Long =
        memos.computeIfAbsent(r, resource => d.write(ResourceMarshaler.convert(strings, resource)))

object ResourceMarshaler:
    def convert(strings: StringDictionary, resource: Resource): opentelemetry.proto.mmap.v1.Mmap.Resource =
      val b = opentelemetry.proto.mmap.v1.Mmap.Resource.newBuilder()
      resource.getAttributes().forEach((k,v) => b.addAttributes(AttributeHelper.convertKv(strings)(k,v)))
      b.build()