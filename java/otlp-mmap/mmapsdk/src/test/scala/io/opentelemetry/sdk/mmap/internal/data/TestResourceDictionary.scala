package io.opentelemetry.sdk.mmap.internal
package data

import munit.FunSuite
import java.io.RandomAccessFile
import java.nio.file.Files
import java.util.HexFormat
import java.nio.file.Paths
import io.opentelemetry.api.common.Attributes

class TestResourceDictionary extends FunSuite:
    test("basic resource dictionary writes") {
        val file = java.io.File.createTempFile("resource-dictionary", "otlp");
        file.deleteOnExit()
        val raf = new RandomAccessFile(file, "rw")
        val d = Dictionary(raf.getChannel(), 0)
        val sd = StringDictionary(d)
        val rd = ResourceDictionary(d, sd)
        val r1 = Resource(Attributes.builder.put("test", 1).build())
        val idx = rd.intern(r1)
        val r2 = Resource(Attributes.builder
                    .put("test1", 1)
                    .put("test2", 2)
                    .put("test3", 3)
                    .put("test4", 4)
                    .build())
        val idx2 = rd.intern(r2)
        assertEquals(rd.intern(r1), idx, "Failed to return same index for same resource.")
        // TODO - we need to check actual bytes or reads.
        given SizedReadable[opentelemetry.proto.mmap.v1.Mmap.Resource] = ProtoReader(opentelemetry.proto.mmap.v1.Mmap.Resource.getDefaultInstance())
        val resource_entry = d.read[opentelemetry.proto.mmap.v1.Mmap.Resource](idx)
        assertEquals(resource_entry.getAttributesCount(), 1)
        val resource_entry2 = d.read[opentelemetry.proto.mmap.v1.Mmap.Resource](idx2)
        assertEquals(resource_entry2.getAttributesCount(), 4)
        // val bytes = Files.readAllBytes(Paths.get(file.getAbsolutePath()))
        // val hex = HexFormat.of().formatHex(bytes)
        // println(hex)
    }
