package io.opentelemetry.sdk.mmap.internal

import java.lang.foreign.Arena
import java.lang.foreign.MemorySegment
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.lang.invoke.VarHandle
import scala.compiletime.ops.boolean

/** Options for creating a ring buffer. */
case class RingBufferOptions(
    /** Buffer size, in bytes. */
    buffer_size: Long,
    /** Number of buffers in the ring. */
    num_buffers: Long,
)

/** 
 * A header for a ringbuffer in the file.
 * 
 * This provides read/write access and memory synchronization primitives.
 */
final class RingBufferHeader(val segment: MemorySegment) extends Header:
    val num_buffers = MetadataLongField(0)
    val buffer_size = MetadataLongField(8)
    val read_position = MetadataLongField(16)
    val write_position = MetadataLongField(24)

final class RingBufferAvailability(val segment: MemorySegment, buffer_size: Int) extends Header:
  val availability = MetadataIntArray(0, buffer_size)
  val shift = log2(buffer_size)

  // We need to fill the availability buffer with sentinel values.
  for idx <- (0 until buffer_size)
  do availability(idx) = -1

  // TODO assumes power of two for index size.
  // val mask = buffer_size-1

  /** Marks a ring buffer available for reading. */
  def setAvailable(idx: Long): Unit =
    // Note: the header library uses "setRelease" for array assignment.
    availability(ringIndexFromRawIndex(idx)) = availabilityFlagForIndex(idx)

  /** Returns true if the given ring buffer index is available for reading. */
  def isAvailable(idx: Long): Boolean =
    val ridx = ringIndexFromRawIndex(idx)
    val flag = availabilityFlagForIndex(idx)
    // Note: Header library use `getAcquire` for array access.
    availability(ridx) == flag

  /** 
   * Calculates the next readable buffer.
   * 
   * @param lowerBound This should be the first possible sequence
   *                   that could be read (reader_position+1)
   * @param available This should be the last possible written
   *                  position (writer_position).
   */
  def getNextReadable(firstAvailable: Long, lastAvailable: Long): Long =
    // We look for the first index that is NOT available,
    // and return the previous index.
    firstAvailable.until(lastAvailable)
    .find(idx => !isAvailable(idx))
    .map(a => a - 1)
    .getOrElse(lastAvailable)
  

  /** Accepts a monotonically increasing index of "event"
   * and returns the value for the number of times this index
   * would "wrap" around a ring buffer.
   */
  private def availabilityFlagForIndex(idx: Long): Int =
    (idx >>> shift).toInt
  private def ringIndexFromRawIndex(idx: Long): Int =
    (idx % buffer_size).toInt
    // TODO - force power of two (idx.toInt & mask )

/**
  * An in-memory ring-buffer that will use primitives against the header
  * to write to each ring buffer chunk.
  *
  * @param header A wrapper around the memory segment representing the header.
  * @param chunks The memory segments we use for each chunk in the ringbuffer.
  */
final class RingBuffer(
  header: RingBufferHeader,
  availability: RingBufferAvailability,
  chunks: Array[MemorySegment]):

  /** Write a chunk to the ring buffer. */
  def writeChunk[A](writer: ByteBuffer => A): A =
    // we may need to do a double-lock approach for MPSC style,
    // where we first spin-lock for write access to a buffer, and then
    // give over access to the buffer to the writer later.
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
      val end = header.read_position.getVolatile()
      val current = header.write_position.get()
      val next = (current + 1) % header.num_buffers.get()
      if (next != end) && header.write_position.compareAndSet(current, next)
      then 
        currentIndex = next.toInt
        true
      else false
    // TODO - exponential backoff
    while !tryMoveNextChunk()
    do Thread.`yield`()

object RingBuffer:
    private val HEADER_SIZE = 32
    def apply(channel: FileChannel, offset: Long, opt: RingBufferOptions): RingBuffer =
        // TODO - validation on options.
        val arena = Arena.ofConfined()
        println(s"Creating ring buffer header from ${offset} to ${offset+HEADER_SIZE}")
        val header = RingBufferHeader(channel.map(MapMode.READ_WRITE, offset, HEADER_SIZE, arena))
        header.buffer_size.set(opt.buffer_size)
        header.num_buffers.set(opt.num_buffers)
        header.read_position.set(0)
        header.write_position.set(0)
        // next create availability array.
        val availability_bytes = 4*opt.buffer_size
        val availability_offset = offset+HEADER_SIZE
        val availability = RingBufferAvailability(
          channel.map(MapMode.READ_WRITE, availability_offset, availability_bytes, arena),
          opt.buffer_size.toInt)
        val ring_buffers_offset = availability_offset + availability_bytes
        val buffers = 
            (0 until opt.num_buffers.toInt).map: i =>
                val chunk_start = ring_buffers_offset+(opt.buffer_size*i)
                channel.map(MapMode.READ_WRITE, chunk_start, opt.buffer_size, arena)
            .toArray
        buffers.foreach(println)
        new RingBuffer(header, availability, buffers)