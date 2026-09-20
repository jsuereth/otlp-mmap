package io.opentelemetry.sdk.mmap.internal
package data

import io.opentelemetry.api.common.AttributeType
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Value
import io.opentelemetry.api.common.ValueType
import io.opentelemetry.api.common.KeyValue
import com.google.protobuf.ByteString
import scala.jdk.CollectionConverters.*
import opentelemetry.proto.mmap.v1.Mmap

object AttributeHelper:
    def convertValue(strings: StringDictionary, value: Value[?], builder: Mmap.AnyValue.Builder): Unit =
      (value.getType: @unchecked) match
        case ValueType.STRING =>
          builder.setStringValue(value.getValue.asInstanceOf[String])
        case ValueType.BOOLEAN =>
          builder.setBoolValue(value.getValue.asInstanceOf[Boolean])
        case ValueType.LONG =>
          builder.setIntValue(value.getValue.asInstanceOf[Long])
        case ValueType.DOUBLE =>
          builder.setDoubleValue(value.getValue.asInstanceOf[Double])
        case ValueType.BYTES =>
          val bytes = value.getValue match
            case b: Array[Byte] => ByteString.copyFrom(b)
            case bb: java.nio.ByteBuffer => ByteString.copyFrom(bb)
            case other => throw new UnsupportedOperationException(s"Unsupported bytes representation: ${other.getClass}")
          builder.setBytesValue(bytes)
        case ValueType.ARRAY =>
          val arrayBuilder = builder.getArrayValueBuilder()
          val list = value.getValue.asInstanceOf[java.util.List[Value[?]]]
          list.forEach { item =>
            convertValue(strings, item, arrayBuilder.addValuesBuilder())
          }
        case ValueType.KEY_VALUE_LIST =>
          val kvlistBuilder = builder.getKvlistValueBuilder()
          val list = value.getValue.asInstanceOf[java.util.List[KeyValue]]
          list.forEach { kv =>
            val refBuilder = kvlistBuilder.addValuesBuilder()
            refBuilder.setKeyRef(strings.intern(kv.getKey()))
            convertValue(strings, kv.getValue(), refBuilder.getValueBuilder())
          }
        case ValueType.EMPTY =>
          // Leave AnyValue empty
        case other =>
          throw new UnsupportedOperationException(s"Unsupported ValueType: ${other}")

    def convertKv(strings: StringDictionary)(k: AttributeKey[?], v: Any): Mmap.KeyValueRef =
        val kv = Mmap.KeyValueRef.newBuilder()
        kv.setKeyRef(strings.intern(k.getKey()))
        (k.getType: @unchecked) match
          case AttributeType.BOOLEAN => kv.getValueBuilder().setBoolValue(v.asInstanceOf[Boolean])
          case AttributeType.STRING => kv.getValueBuilder().setStringValue(v.asInstanceOf[String])
          case AttributeType.LONG => kv.getValueBuilder().setIntValue(v.asInstanceOf[Long])
          case AttributeType.DOUBLE => kv.getValueBuilder().setDoubleValue(v.asInstanceOf[Double])
          case AttributeType.STRING_ARRAY => 
            val builder = kv.getValueBuilder().getArrayValueBuilder()
            v.asInstanceOf[java.util.List[String]].forEach(s => builder.addValues(Mmap.AnyValue.newBuilder().setStringValue(s)))
          case AttributeType.BOOLEAN_ARRAY => 
            val builder = kv.getValueBuilder().getArrayValueBuilder()
            v.asInstanceOf[java.util.List[Boolean]].forEach(b => builder.addValues(Mmap.AnyValue.newBuilder().setBoolValue(b)))
          case AttributeType.LONG_ARRAY => 
            val builder = kv.getValueBuilder().getArrayValueBuilder()
            v.asInstanceOf[java.util.List[Long]].forEach(l => builder.addValues(Mmap.AnyValue.newBuilder().setIntValue(l)))
          case AttributeType.DOUBLE_ARRAY => 
            val builder = kv.getValueBuilder().getArrayValueBuilder()
            v.asInstanceOf[java.util.List[Double]].forEach(d => builder.addValues(Mmap.AnyValue.newBuilder().setDoubleValue(d)))
          case AttributeType.VALUE =>
            convertValue(strings, v.asInstanceOf[Value[?]], kv.getValueBuilder())
          case other =>
            throw new UnsupportedOperationException(s"Unsupported attribute type: ${other}")
        kv.build()
