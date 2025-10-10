package io.opentelemetry.sdk.mmap.internal

import munit.*
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path

// An object with known byte layout we use to test file writing.
object TestBytes
given Writable[TestBytes.type] with
    /** Writes the data to a byte buffer. */
    extension (data: TestBytes.type) def write(buffer: ByteBuffer): Unit =
        buffer.put(1.toByte)
        buffer.put(2.toByte)
        buffer.put(3.toByte)
        buffer.put(4.toByte)
        buffer.put(5.toByte)
        buffer.put(6.toByte)
        buffer.put(7.toByte)
        buffer.put(0.toByte)
    /** The size of the value in bytes when serialized. */
    extension (data: TestBytes.type) def size: Long = 8

class TestSdkMmap extends FunSuite:
    test("basic mmap sdk file") {
        val opts = SdkMmapOptions(
            events = RingBufferOptions(TestBytes.size,2),
            measurements = RingBufferOptions(TestBytes.size,2),
            spans = RingBufferOptions(TestBytes.size,2)
        )
        val file = java.io.File.createTempFile("mmap", "otlp");
        file.deleteOnExit()
        val raf = new RandomAccessFile(file, "rw");
        val mmap = SdkMmapRaw(raf, opts)
        println("Map is created")
        System.out.flush()
        // Write to every location a known pattern.
        println("Writing event")
        mmap.write_event(TestBytes)
        println("Writing span")
        mmap.write_span(TestBytes)
        println("Writing measurement")
        mmap.write_measurement(TestBytes)
        println("Writing dictionary entry")
        mmap.write_entry(TestBytes)
        // now flush
        

        // Now check file has things in it.
        val byteArray: Array[Byte] = Files.readAllBytes(Path.of(file.getPath()))
        // TODO - Spot check headers.

        // Event Ring buffer header is at byte 64, each chunk is 8 from there.
        assertEquals(byteArray(128), 1.toByte)
        assertEquals(byteArray(129), 2.toByte)
        assertEquals(byteArray(130), 3.toByte)
        assertEquals(byteArray(131), 4.toByte)

        // Span Ring buffer should have same values.
        assertEquals(byteArray(208), 1.toByte)
        assertEquals(byteArray(209), 2.toByte)
        assertEquals(byteArray(210), 3.toByte)
        assertEquals(byteArray(211), 4.toByte)

        // Measurement Ring buffer should have same values.
        assertEquals(byteArray(288), 1.toByte)
        assertEquals(byteArray(289), 2.toByte)
        assertEquals(byteArray(290), 3.toByte)
        assertEquals(byteArray(291), 4.toByte)


        // Dictionary should have the same value in its first entry.
        assertEquals(byteArray(368), 1.toByte)
        assertEquals(byteArray(369), 2.toByte)
        assertEquals(byteArray(370), 3.toByte)
        assertEquals(byteArray(371), 4.toByte)
    }
