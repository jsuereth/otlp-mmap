package io.opentelemetry.sdk.mmap.internal
package data

import munit.FunSuite
import java.io.RandomAccessFile
import io.opentelemetry.api.common.AttributeKey
import java.util.List as JList

class TestAttributeHelper extends FunSuite:
    test("convert array attributes") {
        val file = java.io.File.createTempFile("attribute-helper", "otlp")
        file.deleteOnExit()
        val raf = new RandomAccessFile(file, "rw")
        val d = Dictionary(raf.getChannel(), 0)
        val sd = StringDictionary(d)

        // String Array
        val sk = AttributeKey.stringArrayKey("string_array")
        val sv = JList.of("a", "b", "c")
        val skv = AttributeHelper.convertKv(sd)(sk, sv)
        assertEquals(skv.getValue().getArrayValue().getValuesCount(), 3)
        assertEquals(skv.getValue().getArrayValue().getValues(0).getStringValue(), "a")

        // Boolean Array
        val bk = AttributeKey.booleanArrayKey("bool_array")
        val bv = JList.of(true, false)
        val bkv = AttributeHelper.convertKv(sd)(bk, bv)
        assertEquals(bkv.getValue().getArrayValue().getValuesCount(), 2)
        assertEquals(bkv.getValue().getArrayValue().getValues(0).getBoolValue(), true)

        // Long Array
        val lk = AttributeKey.longArrayKey("long_array")
        val lv = JList.of(1L, 2L)
        val lkv = AttributeHelper.convertKv(sd)(lk, lv)
        assertEquals(lkv.getValue().getArrayValue().getValuesCount(), 2)
        assertEquals(lkv.getValue().getArrayValue().getValues(0).getIntValue(), 1L)

        // Double Array
        val dk = AttributeKey.doubleArrayKey("double_array")
        val dv = JList.of(1.1, 2.2)
        val dkv = AttributeHelper.convertKv(sd)(dk, dv)
        assertEquals(dkv.getValue().getArrayValue().getValuesCount(), 2)
        assertEquals(dkv.getValue().getArrayValue().getValues(0).getDoubleValue(), 1.1)
    }

    test("convert Value attributes (AttributeType.VALUE)") {
        import io.opentelemetry.api.common.Value
        import io.opentelemetry.api.common.KeyValue
        val file = java.io.File.createTempFile("attribute-helper-val", "otlp")
        file.deleteOnExit()
        val raf = new RandomAccessFile(file, "rw")
        val d = Dictionary(raf.getChannel(), 0)
        val sd = StringDictionary(d)

        // String Value
        val vkStr = AttributeKey.valueKey("val_str")
        val vStr = Value.of("hello world")
        val kvrStr = AttributeHelper.convertKv(sd)(vkStr, vStr)
        assertEquals(kvrStr.getValue().getStringValue(), "hello world")

        // Long Value
        val vkLong = AttributeKey.valueKey("val_long")
        val vLong = Value.of(42L)
        val kvrLong = AttributeHelper.convertKv(sd)(vkLong, vLong)
        assertEquals(kvrLong.getValue().getIntValue(), 42L)

        // Boolean Value
        val vkBool = AttributeKey.valueKey("val_bool")
        val vBool = Value.of(true)
        val kvrBool = AttributeHelper.convertKv(sd)(vkBool, vBool)
        assertEquals(kvrBool.getValue().getBoolValue(), true)

        // Double Value
        val vkDouble = AttributeKey.valueKey("val_double")
        val vDouble = Value.of(3.14)
        val kvrDouble = AttributeHelper.convertKv(sd)(vkDouble, vDouble)
        assertEquals(kvrDouble.getValue().getDoubleValue(), 3.14)

        // Bytes Value
        val vkBytes = AttributeKey.valueKey("val_bytes")
        val vBytes = Value.of(Array[Byte](1, 2, 3))
        val kvrBytes = AttributeHelper.convertKv(sd)(vkBytes, vBytes)
        assertEquals(kvrBytes.getValue().getBytesValue().toByteArray().toSeq, Seq[Byte](1, 2, 3))

        // Array Value
        val vkArray = AttributeKey.valueKey("val_array")
        val vArray = Value.of(Value.of("item1"), Value.of(99L))
        val kvrArray = AttributeHelper.convertKv(sd)(vkArray, vArray)
        assertEquals(kvrArray.getValue().getArrayValue().getValuesCount(), 2)
        assertEquals(kvrArray.getValue().getArrayValue().getValues(0).getStringValue(), "item1")
        assertEquals(kvrArray.getValue().getArrayValue().getValues(1).getIntValue(), 99L)

        // KeyValueList Value (nested)
        val vkKvlist = AttributeKey.valueKey("val_kvlist")
        val vKvlist = Value.of(
          KeyValue.of("sub_key", Value.of("sub_val")),
          KeyValue.of("nested_array", Value.of(Value.of(1L), Value.of(2L)))
        )
        val kvrKvlist = AttributeHelper.convertKv(sd)(vkKvlist, vKvlist)
        val kvlist = kvrKvlist.getValue().getKvlistValue()
        assertEquals(kvlist.getValuesCount(), 2)
        assertEquals(sd.read(kvlist.getValues(0).getKeyRef()), "sub_key")
        assertEquals(kvlist.getValues(0).getValue().getStringValue(), "sub_val")
        assertEquals(sd.read(kvlist.getValues(1).getKeyRef()), "nested_array")
        assertEquals(kvlist.getValues(1).getValue().getArrayValue().getValues(0).getIntValue(), 1L)
        assertEquals(kvlist.getValues(1).getValue().getArrayValue().getValues(1).getIntValue(), 2L)
    }

    test("unsupported attribute throws UnsupportedOperationException") {
        import io.opentelemetry.api.common.AttributeType
        val file = java.io.File.createTempFile("attribute-helper-err", "otlp")
        file.deleteOnExit()
        val raf = new RandomAccessFile(file, "rw")
        val d = Dictionary(raf.getChannel(), 0)
        val sd = StringDictionary(d)

        val fakeKey = new AttributeKey[String]:
          override def getKey(): String = "fake"
          override def getType(): AttributeType = null

        intercept[UnsupportedOperationException] {
          AttributeHelper.convertKv(sd)(fakeKey, "value")
        }
    }

