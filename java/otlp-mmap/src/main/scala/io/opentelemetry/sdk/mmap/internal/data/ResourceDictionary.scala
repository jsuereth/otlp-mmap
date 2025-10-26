package io.opentelemetry.sdk.mmap.internal
package data

import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.exporter.internal.marshal.MarshalerWithSize
import io.opentelemetry.exporter.internal.marshal.Serializer
import io.opentelemetry.exporter.internal.marshal.Marshaler
import io.opentelemetry.exporter.internal.marshal.MarshalerUtil
import java.nio.ByteBuffer
import io.opentelemetry.otlp.mmap.internal.ByteBufferOutputStream


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
        memos.computeIfAbsent(r, resource => d.write(ResourceMarshaler(strings)(resource)))

object ResourceMarshaler:
    def apply(strings: StringDictionary)(resource: Resource): MarshalerWithSize =
        val kvs = new Array[MarshalerWithSize](resource.getAttributes().size())
        var idx = 0
        resource.getAttributes().forEach((k,v) => {
            kvs(idx) = KeyValueMarshaler(strings)(k, v)
            idx +=1
        })
        new ResourceMarshaler(kvs)
    private def calculateSize(attributes: Array[MarshalerWithSize]): Int =
        MarshalerUtil.sizeRepeatedMessage(io.opentelemetry.proto.resource.v1.internal.Resource.ATTRIBUTES, attributes.asInstanceOf[Array[Marshaler]])
class ResourceMarshaler private (attributes: Array[MarshalerWithSize])
  extends MarshalerWithSize(ResourceMarshaler.calculateSize(attributes)):
  override def writeTo(output: Serializer): Unit =
    output.serializeRepeatedMessage(io.opentelemetry.proto.resource.v1.internal.Resource.ATTRIBUTES, attributes.asInstanceOf[Array[Marshaler]])