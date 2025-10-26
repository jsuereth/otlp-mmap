package io.opentelemetry.sdk.mmap.internal
package data

import munit.FunSuite
import java.io.RandomAccessFile
import io.opentelemetry.sdk.resources.Resource
import java.nio.file.Files
import java.util.HexFormat
import java.nio.file.Paths

class TestResourceDictionary extends FunSuite:
    test("basic resource dictionary writes") {
        val file = java.io.File.createTempFile("resource-dictionary", "otlp");
        file.deleteOnExit()
        val raf = new RandomAccessFile(file, "rw")
        val d = Dictionary(raf.getChannel(), 0)
        val sd = StringDictionary(d)
        val rd = ResourceDictionary(d, sd)
        val r1 = Resource.builder.put("test", 1).build()
        val idx = rd.intern(r1)
        val r2 = Resource.getDefault()
        val idx2 = rd.intern(r2)
        assertEquals(rd.intern(r1), idx, "Failed to return same index for same resource.")
        // TODO - we need to check actual bytes.
        val bytes = Files.readAllBytes(Paths.get(file.getAbsolutePath()))
        val hex = HexFormat.of().formatHex(bytes)
        println(hex)
    }
