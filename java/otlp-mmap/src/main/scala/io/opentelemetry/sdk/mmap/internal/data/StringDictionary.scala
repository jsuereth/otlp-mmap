package io.opentelemetry.sdk.mmap.internal
package data

import java.nio.ByteBuffer
import io.opentelemetry.sdk.mmap.internal.ByteBufferOutputStream
import java.nio.charset.StandardCharsets


// Writer for String, in protobuf format.
// - varint length of string
// - UTF-8 bytes of the string.
given Writable[String] with
    extension (data: String) def write(buffer: ByteBuffer): Unit = 
        val bytes = data.getBytes(java.nio.charset.StandardCharsets.UTF_8)
        val out = ByteBufferOutputStream(buffer)
        VarInt.writeVarInt64(bytes.length, out)
        out.write(bytes)
    extension (data: String) def size: Long = 
        val bytes = data.getBytes(java.nio.charset.StandardCharsets.UTF_8)
        // Our varIntLength function returns the number of ADDITIONAL bytes needed.
        VarInt.sizeVarInt64(bytes.length) + bytes.length
    // Note - This will avoid calculating UTF-8 bytes multiple times, which can be expensive in Java.
    extension (data: String) override def intern(dict: Dictionary): Long =
        val bytes = data.getBytes(java.nio.charset.StandardCharsets.UTF_8)
        val size = VarInt.sizeVarInt64(bytes.length) + bytes.length
        dict.writeEntry(size) { buffer =>
            val out = ByteBufferOutputStream(buffer)
            VarInt.writeVarInt64(bytes.length, out)
            out.write(bytes)
        }

// TODO - speed this up.
given SizedReadable[String]:
  override def read(size: Long, buffer: ByteBuffer): String =
     val cbuf = new Array[Byte](size.toInt)
     buffer.get(cbuf)
     new String(cbuf, StandardCharsets.UTF_8)

/** A Dictionary that can remember strings by an index. */
final class StringDictionary(d: Dictionary):
    // TODO - We'll want some kind of limit to avoid OOM-ing.
    // We also need to benchmark memory overhead of this *specific* region if possible.
    private val memos = java.util.concurrent.ConcurrentHashMap[String,Long]()
    /** Adds (or returns previously added) index of a string in the dictionary. */
    def intern(value: String): Long = memos.computeIfAbsent(value, d.write)
    /** Reads the serialized value. */
    def read(location: Long): String = d.read(location)
