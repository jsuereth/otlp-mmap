package io.opentelemetry.sdk.mmap.internal

import java.lang.foreign.Arena
import java.lang.foreign.MemorySegment
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode

/** 
 * A header for the dictionary in the file.
 * 
 * This provides read/write access and memory synchronization primitives.
 */
final class DictionaryHeader(val segment: MemorySegment) extends Header:
    val end = MetadataLongField(0)
    val num_entries = MetadataLongField(8)

final class Dictionary(header: DictionaryHeader, channel: FileChannel):
    def write[A: Writable](value: A): Long = value.intern(this)
    def writeEntry(size: Long)(writer: ByteBuffer => Unit): Long =
        // Reserve space for the next entry.
        val id = header.end.getAndAdd(size)
        // TODO - make this thread safe?
        try writer(channel.map(MapMode.READ_WRITE, id, size))
        finally header.num_entries.setRelease(header.num_entries.get()+1)
        id
    def force(): Unit =
        header.force()

    def read[A: SizedReadable](location: Long): A =
        readEntry(location, summon[SizedReadable[A]].read)
    // For testing - Reads length-delimited entry
    // Reader takes a "size" and the bytebuffer containing the entry.
    def readEntry[T](location: Long, reader: (Long, ByteBuffer) => T): T =
        // First read the header, maximum 10 bytes for large 64-bit integers
        val buf = channel.map(MapMode.READ_ONLY, location, 10)
        val size = VarInt.readVarInt64(buf)
        val numByteSkip = VarInt.sizeVarInt64(size)
        // Now read the actual message
        val msg = channel.map(MapMode.READ_ONLY, location+numByteSkip, size)
        reader(size, msg)

object Dictionary:
    private val HEADER_SIZE = 64
    def apply(channel: FileChannel, offset: Long): Dictionary =
        val arena = Arena.ofShared()
        val header = DictionaryHeader(channel.map(MapMode.READ_WRITE, offset, HEADER_SIZE, arena))
        // TODO - reload on crash?
        header.num_entries.set(0)
        // Make sure we start after the dictionary header...
        header.end.set(offset+HEADER_SIZE)
        new Dictionary(header, channel)
