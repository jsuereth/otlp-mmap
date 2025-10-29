package io.opentelemetry.sdk.mmap.internal
package data

import io.opentelemetry.api.common.AttributeType
import io.opentelemetry.api.common.AttributeKey

object AttributeHelper:
    def convertKv(strings: StringDictionary)(k: AttributeKey[_], v: Any): opentelemetry.proto.mmap.v1.Mmap.KeyValueRef =
        val kv = opentelemetry.proto.mmap.v1.Mmap.KeyValueRef.newBuilder()
        kv.setKeyRef(strings.intern(k.getKey()))
        k.getType match
          case AttributeType.BOOLEAN => kv.getValueBuilder().setBoolValue(v.asInstanceOf[Boolean])
          case AttributeType.STRING => kv.getValueBuilder().setStringValue(v.asInstanceOf[String])
          case AttributeType.LONG => kv.getValueBuilder().setIntValue(v.asInstanceOf[Long])
          case AttributeType.DOUBLE => kv.getValueBuilder().setDoubleValue(v.asInstanceOf[Double])
          // TODO - handle lists.
          case AttributeType.STRING_ARRAY => ???
          case AttributeType.BOOLEAN_ARRAY => ???
          case AttributeType.LONG_ARRAY => ???
          case AttributeType.DOUBLE_ARRAY => ???
        kv.build()


