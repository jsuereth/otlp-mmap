package io.opentelemetry.sdk.mmap.internal
package data

import io.opentelemetry.proto.common.v1.internal.AnyValue
import io.opentelemetry.exporter.internal.marshal.MarshalerWithSize
import io.opentelemetry.exporter.internal.marshal.Serializer
import io.opentelemetry.exporter.internal.marshal.CodedOutputStream
import java.nio.charset.StandardCharsets

// We copy-paste a lot of Java's marshalling code because it's private.
object AnyValueMarshaler:
    def apply[T](in: T): MarshalerWithSize =
        in match
            case value: String => StringAnyValueMarshaler(value)
            case value: Int => IntAnyValueMarshaler(value)
            case value: Long => IntAnyValueMarshaler(value)
            case value: Float => DoubleAnyValueMarshaler(value)
            case value: Double => DoubleAnyValueMarshaler(value)
            case value: Array[Byte] => BytesAnyValueWriter(value)
            // TODO - Arrays + Maps
            case _ => 
                throw new RuntimeException(s"Unsupported type: ${in.getClass}, ${in}")
        
object StringAnyValueMarshaler:
    def apply(in: String): StringAnyValueMarshaler =
        new StringAnyValueMarshaler(in.getBytes(StandardCharsets.UTF_8))
    private def calculateSize(valueUtf8: Array[Byte]): Int =
        if valueUtf8.length == 0 then 0
        else AnyValue.STRING_VALUE.getTagSize() + CodedOutputStream.computeByteArraySizeNoTag(valueUtf8)
class StringAnyValueMarshaler(valueUtf8: Array[Byte]) extends MarshalerWithSize(StringAnyValueMarshaler.calculateSize(valueUtf8)):
    override def writeTo(output: Serializer): Unit =
        if valueUtf8.length != 0 then
            output.writeString(AnyValue.STRING_VALUE, valueUtf8)

object IntAnyValueMarshaler:
    private def calculateSize(value: Long): Int =
        AnyValue.INT_VALUE.getTagSize() + CodedOutputStream.computeInt64SizeNoTag(value)
class IntAnyValueMarshaler(value: Long) extends MarshalerWithSize(IntAnyValueMarshaler.calculateSize(value)):
  override def writeTo(output: Serializer): Unit =
    output.writeInt64(AnyValue.INT_VALUE, value)

object DoubleAnyValueMarshaler:
    private def calculateSize(value: Double): Int =
        AnyValue.DOUBLE_VALUE.getTagSize() + CodedOutputStream.computeDoubleSizeNoTag(value)
class DoubleAnyValueMarshaler(value: Double) extends MarshalerWithSize(DoubleAnyValueMarshaler.calculateSize(value)):
  override def writeTo(output: Serializer): Unit =
    output.writeDouble(AnyValue.DOUBLE_VALUE, value)

object BytesAnyValueWriter:
    private def calculateSize(value: Array[Byte]): Int =
        if value.length == 0 then 0
        else AnyValue.BYTES_VALUE.getTagSize() + CodedOutputStream.computeByteArraySizeNoTag(value)
class BytesAnyValueWriter(value: Array[Byte]) extends MarshalerWithSize(BytesAnyValueWriter.calculateSize(value)):
    override def writeTo(output: Serializer): Unit =
        if value.length != 0 then
            output.writeString(AnyValue.BYTES_VALUE, value)