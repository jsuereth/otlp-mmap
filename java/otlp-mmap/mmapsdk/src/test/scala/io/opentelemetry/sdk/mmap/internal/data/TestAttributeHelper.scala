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
