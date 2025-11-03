package io.opentelemetry.sdk.mmap.internal

import java.lang.foreign.ValueLayout
import java.lang.foreign.MemorySegment
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.lang.foreign.Arena
import java.io.RandomAccessFile
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.sdk.mmap.internal.data.StringDictionary
import io.opentelemetry.sdk.mmap.internal.data.ResourceDictionary
import io.opentelemetry.sdk.mmap.internal.data.ScopeDictionary
import io.opentelemetry.sdk.mmap.internal.data.MetricDictionary
import java.time.Instant

class SdkMmap(raw: SdkMmapRaw):
    // Wrapper methods around SDK mmap file.

    /** Obtains the attribute index in the dictionary for a set of attributes. */
    def lookupAttributeRef(attributes: Attributes): Long = ???


class FileHeader(val segment: MemorySegment) extends Header:
    val version = MetadataLongField(0)
    val events = MetadataLongField(1*8)
    val spans = MetadataLongField(2*8)
    val measurements = MetadataLongField(3*8)
    val dictionary = MetadataLongField(4*8)
    val start_time = MetadataLongField(5*8)
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
    val events: RingBuffer,
    val spans: RingBuffer,
    val measurements: RingBuffer,
    dictionary: Dictionary):

    val strings = StringDictionary(dictionary)
    val resources = ResourceDictionary(dictionary, strings)
    val scopes = ScopeDictionary(dictionary, strings)
    val metrics = MetricDictionary(dictionary, strings)
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
        header.start_time.set(convertInstant(Instant.now()))
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