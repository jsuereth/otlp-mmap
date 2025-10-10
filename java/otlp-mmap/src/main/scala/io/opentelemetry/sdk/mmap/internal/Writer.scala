package io.opentelemetry.sdk.mmap.internal

import java.nio.ByteBuffer

/** Type trait representing the ability to write something. */
trait Writable[T]:
    /** Writes the data to a byte buffer. */
    extension (data: T) def write(buffer: ByteBuffer): Unit
    /** The size of the value in bytes when serialized. */
    extension (data: T) def size: Long