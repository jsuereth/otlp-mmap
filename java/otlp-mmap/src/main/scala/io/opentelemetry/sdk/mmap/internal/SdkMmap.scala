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

/** 
 * A header for the dictionary in the file.
 * 
 * This provides read/write access and memory synchronization primitives.
 */
final class DictionaryHeader(val segment: MemorySegment) extends Header:
    val end = MetadataLongField(0)
    val num_entries = MetadataLongField(8)

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


class FileHeader(val segment: MemorySegment) extends Header:
    val version = MetadataLongField(0)
    val events = MetadataLongField(1*8)
    val spans = MetadataLongField(2*8)
    val measurements = MetadataLongField(3*8)
    val dictionary = MetadataLongField(4*8)
object FileHeader:
    def apply(channel: FileChannel): FileHeader =
        val arena = Arena.ofConfined()
        new FileHeader(channel.map(MapMode.READ_WRITE, 0, 64, arena))

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
        events.writeToNextBuffer(entry.write)

    def write_span[T: Writable](entry: T): Unit =
        spans.writeToNextBuffer(entry.write)

    def write_measurement[T: Writable](entry: T): Unit =
        measurements.writeToNextBuffer(entry.write)

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
        offset += events.byteSize()
        // We need to align this on a 8-byte boundary.
        println(s"Creating span channel @ ${offset}")
        val spans = RingBuffer(file.getChannel(), offset, opt.spans)
        header.spans.set(offset)
        offset += spans.byteSize()
        println(s"Creating measurement channel @ ${offset}")
        val measurements = RingBuffer(file.getChannel(), offset, opt.measurements)
        header.measurements.set(offset)
        offset += measurements.byteSize()
        println(s"Creating dictionary @ ${offset}")
        val dictionary = Dictionary(file.getChannel(), offset)
        header.dictionary.set(offset)
        new SdkMmapRaw(events, spans, measurements, dictionary)