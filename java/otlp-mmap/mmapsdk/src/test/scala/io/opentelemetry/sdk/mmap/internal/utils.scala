package io.opentelemetry.sdk.mmap.internal

import java.nio.ByteBuffer

// Writer for Long
given Writable[Long] with
    extension (data: Long) def write(buffer: ByteBuffer): Unit =
        buffer.putLong(data)
    extension (data: Long) def size: Long = 8
// Reader for Long
given Readable[Long] with
    def read(buffer: ByteBuffer): Long =
        buffer.getLong()

given Writable[Byte] with
    extension (data: Byte) def write(buffer: ByteBuffer): Unit =
        buffer.put(data)
    extension (data: Byte) def size: Long = 1

given Readable[Byte] with
    def read(buffer: ByteBuffer): Byte =
        buffer.get()

// An object with known byte layout we use to test file writing.
object TestBytes
given Writable[TestBytes.type] with
    /** Writes the data to a byte buffer. */
    extension (data: TestBytes.type) def write(buffer: ByteBuffer): Unit =
        buffer.put(1.toByte)
        buffer.put(2.toByte)
        buffer.put(3.toByte)
        buffer.put(4.toByte)
        buffer.put(5.toByte)
        buffer.put(6.toByte)
        buffer.put(7.toByte)
        buffer.put(0.toByte)
    /** The size of the value in bytes when serialized. */
    extension (data: TestBytes.type) def size: Long = 8