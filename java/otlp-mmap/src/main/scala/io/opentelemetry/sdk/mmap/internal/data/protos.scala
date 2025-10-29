package io.opentelemetry.sdk.mmap.internal
package data

import com.google.protobuf.MessageLite
import java.nio.ByteBuffer
import io.opentelemetry.otlp.mmap.internal.ByteBufferOutputStream

given [T <: MessageLite]: Writable[T] with
  extension (data: T) override def write(buffer: ByteBuffer): Unit = 
    data.writeDelimitedTo(new ByteBufferOutputStream(buffer))
  extension (data: T) override def size: Long =
    data.getSerializedSize() + VarInt.sizeVarInt64(data.getSerializedSize())

class ProtoReader[Msg <: MessageLite](base: Msg) 
  extends Readable[Msg] with SizedReadable[Msg]:

  override def read(size: Long, buffer: ByteBuffer): Msg = 
    // TODO - speed up
    val buf = new Array[Byte](size.toInt)
    buffer.get(buf)
    base.newBuilderForType().mergeFrom(buf).build().asInstanceOf[Msg]

  override def read(buffer: ByteBuffer): Msg =
    // TODO - byte buffer read
    // base.newBuilderForType().mergeDelimitedFrom()
    ???