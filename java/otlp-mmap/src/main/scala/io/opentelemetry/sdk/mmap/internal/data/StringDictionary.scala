package io.opentelemetry.sdk.mmap.internal
package data

import java.nio.ByteBuffer
import io.opentelemetry.otlp.mmap.internal.ByteBufferOutputStream


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
        // TODO - avoid calculating utf-8 multiple times.
        val bytes = data.getBytes(java.nio.charset.StandardCharsets.UTF_8)
        // Our varIntLength function returns the number of ADDITIONAL bytes needed.
        1+VarInt.varIntLength(bytes.length) + bytes.length

/** A Dictionary that can remember strings by an index. */
final class StringDictionary(d: Dictionary):
    private val memos = java.util.concurrent.ConcurrentHashMap[String,Long]()
    /** Adds (or returns previously added) index of a string in the dictionary. */
    def intern(value: String): Long = memos.computeIfAbsent(value, d.write)
