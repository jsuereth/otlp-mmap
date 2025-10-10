package io.opentelemetry.sdk.mmap.internal

import java.lang.foreign.ValueLayout
import java.lang.foreign.MemorySegment
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.lang.foreign.Arena
import java.io.RandomAccessFile

class SdkMmap


object Header:
    val metaHandle = ValueLayout.JAVA_LONG.arrayElementVarHandle()

trait Header:
  def segment: MemorySegment
  def force(): Unit = segment.force()
  class MetadataLongField(index: Int):
    inline def get(): Long = Header.metaHandle.get(segment, 0L, index)
    inline def getVolate(): Long = Header.metaHandle.getVolatile(segment, 0L,index)
    inline def set(value: Long) = Header.metaHandle.set(segment, 0L,index, value)
    inline def setVolatile(value: Long) = Header.metaHandle.setVolatile(segment, 0L,index, value)
    inline def setRelease(value: Long) = Header.metaHandle.setRelease(segment, 0L,index, value)
    inline def compareAndSet(expected: Long, value: Long): Boolean = Header.metaHandle.compareAndSet(segment, 0L, index, expected, value)

/** 
 * A header for the dictionary in the file.
 * 
 * This provides read/write access and memory synchronization primitives.
 */
final class DictionaryHeader(val segment: MemorySegment) extends Header:
    val end = MetadataLongField(0)
    val num_entries = MetadataLongField(1)

final class Dictionary(header: DictionaryHeader, channel: FileChannel):
    def writeEntry(size: Long)(writer: ByteBuffer => Unit): Long =
        val id = header.end.get()
        val next_end = size + id
        // TODO - make this thread safe?
        try writer(channel.map(MapMode.READ_WRITE, id, size))
        finally header.end.setRelease(next_end)
        header.num_entries.setRelease(header.num_entries.get()+1)
        id
    def force(): Unit =
        header.force()

object Dictionary:
    def apply(channel: FileChannel, offset: Long): Dictionary =
        println(s"Creating dictionary header from ${offset} to ${offset+64}")
        val arena = Arena.ofConfined()
        val header = DictionaryHeader(channel.map(MapMode.READ_WRITE, offset, 64, arena))
        // TODO - reload on crash?
        header.num_entries.set(0)
        // Make sure we start after the dictionary header...
        header.end.set(offset+64)
        new Dictionary(header, channel)

/** 
 * A header for a ringbuffer in the file.
 * 
 * This provides read/write access and memory synchronization primitives.
 */
final class RingBufferHeader(val segment: MemorySegment) extends Header:
    val num_chunks = MetadataLongField(0)
    val chunk_size = MetadataLongField(1)
    val read_position = MetadataLongField(6)
    val write_position = MetadataLongField(7)

/**
  * An in-memory ring-buffer that will use primitives against the header
  * to write to each ring buffer chunk.
  *
  * @param header A wrapper around the memory segment representing the header.
  * @param chunks The memory segments we use for each chunk in the ringbuffer.
  */
final class RingBuffer(header: RingBufferHeader, chunks: Array[MemorySegment]):
  /** Write a chunk to the ring buffer. */
  def writeChunk[A](writer: ByteBuffer => A): A =
    try writer(currentChunk.asByteBuffer().order(ByteOrder.nativeOrder()))
    finally moveNextChunk()
  def force(): Unit =
    header.force()
    chunks.foreach(_.force())
  private var currentIndex = 0
  // Returns a the current chunk for writing.
  private def currentChunk: MemorySegment =
    chunks(currentIndex)
  // Advanced the write position, when able.  THIS WILL BLOCK.
  private def moveNextChunk(): Unit =
    // Note this will block until we can write.
    def tryMoveNextChunk(): Boolean =
      val end = header.read_position.getVolate()
      val current = header.write_position.get()
      val next = (current + 1) % header.num_chunks.get()
      if (next != end) && header.write_position.compareAndSet(current, next)
      then 
        currentIndex = next.toInt
        true
      else false
    // TODO - exponential backoff
    while !tryMoveNextChunk()
    do Thread.`yield`()

object RingBuffer:
    def apply(channel: FileChannel, offset: Long, opt: RingBufferOptions): RingBuffer =
        val arena = Arena.ofConfined()
        println(s"Creating ring buffer header from ${offset} to ${offset+64}")
        val header = RingBufferHeader(channel.map(MapMode.READ_WRITE, offset, 64, arena))
        header.chunk_size.set(opt.chunk_length)
        header.num_chunks.set(opt.num_chunks)
        header.read_position.set(0)
        header.write_position.set(0)
        val chunks = 
            (0 until opt.num_chunks.toInt).map: i =>
                val chunk_start = offset+64+(opt.chunk_length*i)
                val chunk_end = chunk_start+opt.chunk_length
                println(s"Creating ring buffer chunk from ${chunk_start} to ${chunk_end}")
                channel.map(MapMode.READ_WRITE, chunk_start, opt.chunk_length, arena)
            .toArray
        chunks.foreach(println)
        new RingBuffer(header, chunks)

class FileHeader(val segment: MemorySegment) extends Header:
    val version = MetadataLongField(0)
    val events = MetadataLongField(1)
    val spans = MetadataLongField(2)
    val measurements = MetadataLongField(3)
    val dictionary = MetadataLongField(4)
object FileHeader:
    def apply(channel: FileChannel): FileHeader =
        val arena = Arena.ofConfined()
        new FileHeader(channel.map(MapMode.READ_WRITE, 0, 64, arena))

case class RingBufferOptions(
    chunk_length: Long,
    num_chunks: Long,
)
case class SdkMmapOptions(
    events: RingBufferOptions,
    spans: RingBufferOptions,
    measurements: RingBufferOptions,
)

/**
  * Low level class that gives us helper methods to flushing
  * bytes into all the places we need them.
  *
  * @param events
  * @param spans
  * @param measurements
  * @param dictionary
  */
class SdkMmapRaw(
    events: RingBuffer,
    spans: RingBuffer,
    measurements: RingBuffer,
    dictionary: Dictionary):
    /** Adds a new entry to the dictionary, returning its offset. */
    def write_entry[T: Writable](entry: T): Long =
        dictionary.writeEntry(entry.size)(entry.write)

    def write_event[T: Writable](entry: T): Unit =
        events.writeChunk(entry.write)

    def write_span[T: Writable](entry: T): Unit =
        spans.writeChunk(entry.write)

    def write_measurement[T: Writable](entry: T): Unit =
        measurements.writeChunk(entry.write)

    def force(): Unit =
        events.force()
        spans.force()
        measurements.force()
        dictionary.force()


object SdkMmapRaw:
    val SDK_MMAP_VERSION=1
    def apply(
        file: RandomAccessFile,
        opt: SdkMmapOptions): SdkMmapRaw =
        val header = FileHeader(file.getChannel())
        header.version.set(SDK_MMAP_VERSION)
        // TODO - we need to sort out alignment here.
        var offset = 64L
        println(s"Creating event channel @ ${offset}")
        val events = RingBuffer(file.getChannel(), offset, opt.events)
        header.events.set(offset)
        offset += 64+opt.events.chunk_length*opt.events.num_chunks
        // We need to align this on a 8-byte boundary.
        println(s"Creating span channel @ ${offset}")
        val spans = RingBuffer(file.getChannel(), offset, opt.spans)
        header.spans.set(offset)
        offset += 64+opt.spans.chunk_length*opt.spans.num_chunks
        println(s"Creating measurement channel @ ${offset}")
        val measurements = RingBuffer(file.getChannel(), offset, opt.measurements)
        header.measurements.set(offset)
        offset += 64+opt.measurements.chunk_length*opt.measurements.num_chunks
        println(s"Creating dictionary @ ${offset}")
        val dictionary = Dictionary(file.getChannel(), offset)
        header.dictionary.set(offset)
        new SdkMmapRaw(events, spans, measurements, dictionary)