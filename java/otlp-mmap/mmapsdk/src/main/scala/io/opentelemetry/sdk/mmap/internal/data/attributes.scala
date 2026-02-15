package io.opentelemetry.sdk.mmap.internal
package data

import io.opentelemetry.api.common.AttributeType
import io.opentelemetry.api.common.AttributeKey
import scala.jdk.CollectionConverters.*

object AttributeHelper:
    def convertKv(strings: StringDictionary)(k: AttributeKey[?], v: Any): opentelemetry.proto.mmap.v1.Mmap.KeyValueRef =
        val kv = opentelemetry.proto.mmap.v1.Mmap.KeyValueRef.newBuilder()
        kv.setKeyRef(strings.intern(k.getKey()))
        k.getType match
          case AttributeType.BOOLEAN => kv.getValueBuilder().setBoolValue(v.asInstanceOf[Boolean])
          case AttributeType.STRING => kv.getValueBuilder().setStringValue(v.asInstanceOf[String])
          case AttributeType.LONG => kv.getValueBuilder().setIntValue(v.asInstanceOf[Long])
          case AttributeType.DOUBLE => kv.getValueBuilder().setDoubleValue(v.asInstanceOf[Double])
          case AttributeType.STRING_ARRAY => 
            val builder = kv.getValueBuilder().getArrayValueBuilder()
            v.asInstanceOf[java.util.List[String]].forEach(s => builder.addValues(opentelemetry.proto.mmap.v1.Mmap.AnyValue.newBuilder().setStringValue(s)))
          case AttributeType.BOOLEAN_ARRAY => 
            val builder = kv.getValueBuilder().getArrayValueBuilder()
            v.asInstanceOf[java.util.List[Boolean]].forEach(b => builder.addValues(opentelemetry.proto.mmap.v1.Mmap.AnyValue.newBuilder().setBoolValue(b)))
          case AttributeType.LONG_ARRAY => 
            val builder = kv.getValueBuilder().getArrayValueBuilder()
            v.asInstanceOf[java.util.List[Long]].forEach(l => builder.addValues(opentelemetry.proto.mmap.v1.Mmap.AnyValue.newBuilder().setIntValue(l)))
          case AttributeType.DOUBLE_ARRAY => 
            val builder = kv.getValueBuilder().getArrayValueBuilder()
            v.asInstanceOf[java.util.List[Double]].forEach(d => builder.addValues(opentelemetry.proto.mmap.v1.Mmap.AnyValue.newBuilder().setDoubleValue(d)))
        kv.build()
