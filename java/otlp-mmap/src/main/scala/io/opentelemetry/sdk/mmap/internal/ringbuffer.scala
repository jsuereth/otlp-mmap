package io.opentelemetry.sdk.mmap.internal

import java.lang.foreign.Arena
import java.lang.foreign.MemorySegment
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.lang.invoke.VarHandle
import scala.compiletime.ops.boolean
import java.util.concurrent.atomic.AtomicLong

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

  /** Accepts a monotonically increasing index of "event"
   * and returns the value for the number of times this index
   * would "wrap" around a ring buffer.
   */
  private def availabilityFlagForIndex(idx: Long): Int =
    (idx >>> shift).toInt
  def ringIndexFromRawIndex(idx: Long): Int =
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

  val cachedLastWriterIndex = AtomicLong(-1)

  /** Returns the size, in bytes, this ring buffer takes. */
  def byteSize(): Long =
    // Header
    RingBuffer.HEADER_SIZE +
    // Availability Array
    (4 * header.num_buffers.get()) +
    // Ring buffer
    (header.buffer_size.get() * header.num_buffers.get())

  def hasWriteCapacity(currentIdx: Long): Boolean =
    // We calculate as far "back" in the ring buffer index
    // we can go before we'd overwrite something waiting to be read.
    val previousIndexWithConflict = currentIdx + 1 - header.num_buffers.get()
    val readerPosition = header.read_position.getVolatile()
    // we rely on the reader catching up with the writers to check capacity here.
    previousIndexWithConflict < readerPosition

  /** Attempts to return the next index or None if buffer is full. */
  private def tryObtainNextWrite(): Option[Long] =
    // First we grab the next value.
    val current = header.write_position.get()
    val next = current + 1
    val reader = header.read_position.getVolatile()
    val hasCapacity = hasWriteCapacity(current)
    if hasCapacity && header.write_position.compareAndSet(current, next)
    then Some(next)
    else None

  /** Writes the given data to the next available buffer. */
  def write[T: Writable](data: T): Unit = writeToNextBuffer(data.write)
  /** Writes to the next available buffer in the ring buffer. */
  def writeToNextBuffer[A](writer: ByteBuffer => A) =
    def nextWriteIndex(): Long =
      tryObtainNextWrite() match
        case Some(idx) => idx
        case None => 
          Thread.onSpinWait()
          // TODO - thread.yield or other strategies?
          nextWriteIndex()
    val idx = nextWriteIndex()
    try writer(chunks(availability.ringIndexFromRawIndex(idx)).asByteBuffer().order(ByteOrder.nativeOrder()))
    finally
      availability.setAvailable(idx)

  private def tryObtainNextRead(): Option[Long] =
    val readerPosition = header.read_position.getVolatile()
    val nextRead = readerPosition+1
    if availability.isAvailable(nextRead)
    then Some(nextRead)
    else None

  def readNextBuffer[A: Readable](): A =
    def nextReadIndex(): Long =
      tryObtainNextRead() match
        case Some(idx) => idx
        case None => 
          Thread.onSpinWait()
          nextReadIndex()
    val idx = nextReadIndex()
    try summon[Readable[A]].read(chunks(availability.ringIndexFromRawIndex(idx)).asByteBuffer().order(ByteOrder.nativeOrder()))
    finally header.read_position.setRelease(idx)

  def force(): Unit =
    header.force()
    chunks.foreach(_.force())

object RingBuffer:
    private val HEADER_SIZE = 32
    def apply(channel: FileChannel, offset: Long, opt: RingBufferOptions): RingBuffer =
        // TODO - validation on options.
        val arena = Arena.ofShared()
        val header = RingBufferHeader(channel.map(MapMode.READ_WRITE, offset, HEADER_SIZE, arena))
        header.buffer_size.set(opt.buffer_size)
        header.num_buffers.set(opt.num_buffers)
        header.read_position.set(-1)
        header.write_position.set(-1)
        // next create availability array.
        val availability_bytes = 4*opt.num_buffers
        val availability_offset = offset+HEADER_SIZE
        val availability = RingBufferAvailability(
          channel.map(MapMode.READ_WRITE, availability_offset, availability_bytes, arena),
          opt.num_buffers.toInt)
        val ring_buffers_offset = availability_offset + availability_bytes
        val buffers = 
            (0 until opt.num_buffers.toInt).map: i =>
                val chunk_start = ring_buffers_offset+(opt.buffer_size*i)
                channel.map(MapMode.READ_WRITE, chunk_start, opt.buffer_size, arena)
            .toArray
        new RingBuffer(header, availability, buffers)