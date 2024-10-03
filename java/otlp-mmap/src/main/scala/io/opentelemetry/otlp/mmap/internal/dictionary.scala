package io.opentelemetry.otlp.mmap.internal

import java.lang.foreign.ValueLayout
import java.lang.foreign.MemorySegment
import java.nio.channels.FileChannel
import java.lang.foreign.Arena
import java.nio.channels.FileChannel.MapMode
import java.nio.ByteBuffer

object DictionaryHeader:
    val metaHandle = ValueLayout.JAVA_LONG.arrayElementVarHandle();
    val VERSION_INDEX = 0
    val LENGTH_INDEX = 1
    val SIZE_INDEX = 2

class DictionaryHeader(segment: MemorySegment):
      // Helper class to simplify doing direct memory access using concurrency primitives.
  class MetadataLongField(index: Int):
    inline def get(): Long = RingBufferHeader.metaHandle.get(segment, index)
    inline def getVolate(): Long = RingBufferHeader.metaHandle.getVolatile(segment, index)
    inline def set(value: Long) = RingBufferHeader.metaHandle.set(segment, index, value)
    inline def setVolatile(value: Long) = RingBufferHeader.metaHandle.setVolatile(segment, index, value)
    inline def setRelease(value: Long) = RingBufferHeader.metaHandle.setRelease(segment, index, value)
    inline def compareAndSet(expected: Long, value: Long): Boolean = RingBufferHeader.metaHandle.compareAndSet(segment, index, expected, value)

  val version = MetadataLongField(DictionaryHeader.VERSION_INDEX)
  val length = MetadataLongField(DictionaryHeader.LENGTH_INDEX)
  val chunkSize = MetadataLongField(DictionaryHeader.SIZE_INDEX)

  inline def force(): Unit = segment.force()

  /**
    * An output channel that writes a dictionary.
    * 
    * Entries may be written and will return an ID.  Entries are NOT deduplicated by this channel.
    *
    * @param channel The file channel to write to.
    * @param version The version to use for this file.
    * @param entry_length The maximum length of any entry.
    */
class DictionaryOutputChannel(channel: FileChannel, version: Long, entry_length: Long) extends AutoCloseable:
    // TODO - we don't really need to use mem-mapped files here...

    // Our memory management for these memory segments.
    // TODO - shoudl we use a shared arena?
    private val arena = Arena.ofConfined()
    // The 64-byte header for the ring buffer file.
    private val metadata = DictionaryHeader(channel.map(MapMode.READ_WRITE, 0, 64, arena))

    private def writeHeader(): Unit =
        try
            metadata.version.set(version)
            metadata.chunkSize.set(entry_length)
            metadata.length.set(0)
        catch 
            // TODO - better erorr handling
            case t: Throwable =>
                t.printStackTrace()
                throw t

    /** Writes an entry to the dictionary and returns the ID to use for it. */
    def writeEntry(writer: ByteBuffer => Unit): Long =
        // TODO - make this threadsafe?
        val id = metadata.length.get()
        val chunk_start = 64+(entry_length*id)
        val chunk_end = 64+(entry_length*(id+1))
        try writer(channel.map(MapMode.READ_WRITE, chunk_start, chunk_end))
        finally metadata.length.setRelease(id+1)
        id
        
    override def close(): Unit =
        channel.force(true)