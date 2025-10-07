package io.opentelemetry.otlp.mmap.internal

import java.lang.foreign.ValueLayout
import java.lang.foreign.MemorySegment
import java.nio.channels.FileChannel
import java.io.Closeable
import java.lang.foreign.Arena
import java.nio.channels.FileChannel.MapMode
import scala.collection.mutable.ArrayBuffer
import java.nio.ByteBuffer
import java.nio.ByteOrder

/** Constants for accessing the 64-byte header of a ring buffer file. */
object RingBufferHeader:
  val metaHandle = ValueLayout.JAVA_LONG.arrayElementVarHandle()
  val VERSION_INDEX = 0
  val LENGTH_INDEX = 1
  val CHUNK_SIZE_INDEX = 2
  val READ_POSITION_INDEX = 6
  val WRITE_POSITION_INDEX = 7


/** 
 * A class that gives memory-safe access to the header refion of RingBuffer mmaped file.
 */
class RingBufferHeader(segment: MemorySegment):
  // Helper class to simplify doing direct memory access using concurrency primitives.
  class MetadataLongField(index: Int):
    inline def get(): Long = RingBufferHeader.metaHandle.get(segment, 0L, index.toLong)
    inline def getVolate(): Long = RingBufferHeader.metaHandle.getVolatile(segment, 0L, index.toLong)
    inline def set(value: Long) = RingBufferHeader.metaHandle.set(segment, 0L, index.toLong, value.toLong)
    inline def setVolatile(value: Long) = RingBufferHeader.metaHandle.setVolatile(segment, 0L, index.toLong, value.toLong)
    inline def compareAndSet(expected: Long, value: Long): Boolean = RingBufferHeader.metaHandle.compareAndSet(segment, 0L, index.toLong, expected.toLong, value.toLong)
  /** Version number of the OTLP export */  
  val version = MetadataLongField(RingBufferHeader.VERSION_INDEX)
  /** Number of chunks in the ring buffer. */
  val length = MetadataLongField(RingBufferHeader.LENGTH_INDEX)
  /** Size, in bytes, of chunks of ring buffers. */
  val chunkSize = MetadataLongField(RingBufferHeader.CHUNK_SIZE_INDEX)
  /** Position of the current_chunk about to be written. */
  val writePosition = MetadataLongField(RingBufferHeader.WRITE_POSITION_INDEX)
  /** Position of the current chunk about to be read. */
  val readPosition = MetadataLongField(RingBufferHeader.READ_POSITION_INDEX)
  /** Forces the header to be written to disk. */
  inline def force(): Unit = segment.force()

/** 
 * An output channel that reads chunks from a ring-buffer file.
 * 
 * The output channel decides the size of the chunk and the number of chunks in the ring.
 */
class RingBufferOutputChannel(channel: FileChannel, version: Long, chunk_length: Long, num_chunks: Long) extends Closeable:
  // Our memory management for these memory segments.
  // TODO - shoudl we use a shared arena?
  private val arena = Arena.ofConfined()
  // The 64-byte header for the ring buffer file.
  private val metadata = RingBufferHeader(channel.map(MapMode.READ_WRITE, 0, 64, arena))
  // Thread-local memory of what chunk we're writing to.
  private var currentIndex = 0
  // Allocated ring buffer chunks.
  private var chunks: collection.mutable.ArrayBuffer[MemorySegment] =
    (0 until num_chunks.toInt).map: i =>
      val chunk_start = 64+(chunk_length*i)
      val chunk_end = chunk_start+chunk_length
      channel.map(MapMode.READ_WRITE, chunk_start, chunk_end, arena)
    .to(ArrayBuffer)
  writeHeader()

  // Returns a the current chunk for writing.
  private def currentChunk: MemorySegment =
    chunks(currentIndex)

  // Advanced the write position, when able.  THIS WILL BLOCK.
  private def moveNextChunk(): Unit =
    // Note this will block until we can write.
    def tryMoveNextChunk(): Boolean =
      val end = metadata.readPosition.getVolate()
      val current = metadata.writePosition.get()
      val next = (current + 1) % num_chunks
      if (next != end) && metadata.writePosition.compareAndSet(current, next)
      then 
        currentIndex = next.toInt
        true
      else false
    // TODO - exponential backoff
    while !tryMoveNextChunk()
    do Thread.`yield`()

  // Writes the header
  private def writeHeader(): Unit =
    try
      metadata.version.set(version)
      metadata.chunkSize.set(chunk_length)
      metadata.length.set(num_chunks)
      metadata.readPosition.set(0)
      metadata.writePosition.set(0)
    catch
      // TODO - better error handling.
      case t: Throwable =>
        t.printStackTrace()
        throw t

  /** Write a chunk to the ring buffer. */
  def writeChunk[A](writer: ByteBuffer => A): A =
    try writer(currentChunk.asByteBuffer().order(ByteOrder.nativeOrder()))
    finally moveNextChunk()

  // TODO - allocate ring-buffer.

  // TODO - create next chunk method that will ensure we have a bytebufer at the right piece of memory.

  /** Forces mmap segments to be written to disk. */
  final def force(): Unit =
    metadata.force()
    for chunk <- chunks do chunk.force()

  /** Closes the ring buffer file and cleans up all off-heap memory. */
  override def close(): Unit =
    arena.close()
    channel.force(true)