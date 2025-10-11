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


given Writable[Long] with
    extension (data: Long) def write(buffer: ByteBuffer): Unit =
        buffer.putLong(data)
    extension (data: Long) def size: Long = 8
given Readable[Long] with
    def read(buffer: ByteBuffer): Long =
        buffer.getLong()

class TestRingBuffer extends FunSuite:
    test("basic ringbuffer writes and reads") {
        val file = java.io.File.createTempFile("ringbuffer", "otlp");
        file.deleteOnExit()
        val raf = new RandomAccessFile(file, "rw")
        val options = RingBufferOptions(8,512)
        val buffer = RingBuffer(raf.getChannel(), 0, options)
        // TODO - write a bunch of integers to/from wringbuffer.
        val max = 10000L;
        given ExecutionContext = ExecutionContext.global
        val publish = Future {
            for i <- (0L to max)
            do 
                buffer.write(i)
            ()
        }
        val consume = Future {
            for i <- (0L to max)
            do
                val found = buffer.readNextBuffer[Long]()
                assertEquals(i, found)
        }
        Await.result(consume, 5.seconds)
    }
    test("Multiple threads writing") {
        val file = java.io.File.createTempFile("ringbuffer", "otlp");
        file.deleteOnExit()
        val raf = new RandomAccessFile(file, "rw")
        val options = RingBufferOptions(8,512)
        val buffer = RingBuffer(raf.getChannel(), 0, options)
        // TODO - write a bunch of integers to/from wringbuffer.
        val max = 20000L
        given ExecutionContext = ExecutionContext.global
        val consume = Future {
            val seen = new Array[Boolean](max.toInt+1)
            for i <- (0L to max)
            do
                val found = buffer.readNextBuffer[Long]()
                seen(found.toInt) = true
            seen
        }
        // Make producers across multiple threads
        for range <- (0L to max).grouped(200)
        do Future {
            for i <- range
            do buffer.write(i)

        }
        // Check reuslts
        val result = Await.result(consume, 5.seconds)
        for i <- 0 until result.length
        do
            assertEquals(result(i), true, s"Did not receive msg: ${i}")
    }
