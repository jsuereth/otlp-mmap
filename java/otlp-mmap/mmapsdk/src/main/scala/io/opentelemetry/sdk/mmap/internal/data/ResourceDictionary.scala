package io.opentelemetry.sdk.mmap.internal
package data

import java.nio.ByteBuffer
import io.opentelemetry.sdk.mmap.internal.ByteBufferOutputStream
import io.opentelemetry.api.common.AttributeType

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
      resource.attributes.forEach((k,v) => b.addAttributes(AttributeHelper.convertKv(strings)(k,v)))
      b.build()