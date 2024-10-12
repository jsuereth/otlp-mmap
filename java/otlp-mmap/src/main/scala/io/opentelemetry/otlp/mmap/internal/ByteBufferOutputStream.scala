package io.opentelemetry.otlp.mmap.internal

import java.nio.ByteBuffer
import java.io.OutputStream

/** Helper to ensure we can re-use Java SDK's serialization marshallers for OTLP mmap. */
class ByteBufferOutputStream(buf: ByteBuffer) extends OutputStream():
  override def write(b: Int): Unit = buf.put(b.toByte)
  override def write(b: Array[Byte]): Unit = buf.put(b)
  override def write(b: Array[Byte], off: Int, len: Int): Unit = buf.put(b,off,len)
