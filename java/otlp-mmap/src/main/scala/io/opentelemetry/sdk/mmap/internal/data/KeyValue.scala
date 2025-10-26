package io.opentelemetry.sdk.mmap.internal.data

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.exporter.internal.marshal.MarshalerWithSize
import io.opentelemetry.exporter.internal.marshal.MarshalerUtil
import io.opentelemetry.exporter.internal.marshal.Serializer
import io.opentelemetry.exporter.internal.marshal.ProtoFieldInfo
import io.opentelemetry.sdk.mmap.WireFormat


// Helper to use string interning when serializing key-value pairs.
object KeyValueMarshaler:
    def apply[T](strings: StringDictionary)(key: AttributeKey[T], value: Any): MarshalerWithSize =
        // Grab the index for the key in the dictionary.
        val keyIdx = strings.intern(key.getKey())
        // Now serialize the attribute...
        val valueMarshaler = AnyValueMarshaler(value)
        new KeyValueMarshaler(keyIdx, valueMarshaler)
    def calculateSize(key: Long, value: MarshalerWithSize): Int =
        MarshalerUtil.sizeInt64(KeyValueRef.KEY_REF, key) +
        MarshalerUtil.sizeMessage(KeyValueRef.VALUE, value)
class KeyValueMarshaler private(key: Long, value: MarshalerWithSize) 
    extends MarshalerWithSize(KeyValueMarshaler.calculateSize(key,value)):

  override def writeTo(output: Serializer): Unit =
    output.serializeInt64(KeyValueRef.KEY_REF, key)
    output.serializeMessage(KeyValueRef.VALUE, value)

// proto lookup references
object KeyValueRef:
    val KEY_REF = 
        ProtoFieldInfo.create(
            1,
            WireFormat.VarInt.makeTag(1)
            , "key_ref")
    val VALUE =
        ProtoFieldInfo.create(
            2,
            WireFormat.LengthDelimited.makeTag(2)
            , "value")