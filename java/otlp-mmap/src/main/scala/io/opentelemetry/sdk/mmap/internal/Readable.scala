package io.opentelemetry.sdk.mmap.internal

import java.nio.ByteBuffer

/** Type trait representing the ability to write something. */
trait Readable[T]:
    /** Writes the data to a byte buffer. */
    def read(buffer: ByteBuffer): T
