package io.opentelemetry.sdk.mmap.internal



import munit.*
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scala.concurrent.Await
import scala.concurrent.duration._
import io.opentelemetry.sdk.mmap.internal.data.StringDictionary
import java.nio.charset.StandardCharsets

class TestDictionary extends FunSuite:
    test("basic dictonary writes") {
        val file = java.io.File.createTempFile("dictionary", "otlp");
        file.deleteOnExit()
        val raf = new RandomAccessFile(file, "rw")
        val d = Dictionary(raf.getChannel(), 0)
        val idx = d.write(10L)
        // We know offset is 0, so index should be 64 (past header)
        assertEquals(idx, 64L, "Failed to write first entry after header.")
        val idx2 = d.write(5.toByte)
        assertEquals(idx2, 64L+8, "Failed to write second entry after first")
        val idx3 = d.write(20L)
        assertEquals(idx3, 64L+8+1, "Failed to write third entry after second")

        // TODO - validating reads, may require knowing the length to pull, if we use
        // mmap file for reads.
    }

    test("String intern") {
        val file = java.io.File.createTempFile("string-dictionary", "otlp");
        file.deleteOnExit()
        val raf = new RandomAccessFile(file, "rw")
        val d = Dictionary(raf.getChannel(), 0)
        val sd = StringDictionary(d)
        val idx = sd.intern("Hello")
        assertEquals(idx, 64L, "First elemetn should intern at first position")
        // Next index should be 64+6 bytes away (5 for hello, 1 for size varint)
        val idx2 = sd.intern("second")
        assertEquals(idx2, 70L, "second index should be 'hello' away from first")

        // Now try reading.
        assertEquals(sd.read(idx), "Hello", "Failed to read first interned string")
        assertEquals(sd.read(idx2), "second", "Failed to read second interned string")
    }