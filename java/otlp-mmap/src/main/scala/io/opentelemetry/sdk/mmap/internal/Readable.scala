package io.opentelemetry.sdk.mmap.internal

import java.nio.ByteBuffer

/** Type trait representing the ability to write something. */
trait Readable[T]:
    /** Reads the data from a byte buffer. */
    def read(buffer: ByteBuffer): T

/** Type trait representing the ability to read a sized-prefixed value. */
trait SizedReadable[T]:
    /** Reads the data from a byte buffer, given a size. */
    def read(size: Long, buffer: ByteBuffer): T