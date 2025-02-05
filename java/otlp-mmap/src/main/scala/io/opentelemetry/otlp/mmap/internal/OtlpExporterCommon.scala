package io.opentelemetry.otlp.mmap.internal

import java.io.RandomAccessFile
import scala.collection.mutable.ArrayBuffer
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.sdk.common.InstrumentationScopeInfo
import io.opentelemetry.exporter.internal.otlp.ResourceMarshaler
import io.opentelemetry.exporter.internal.otlp.InstrumentationScopeMarshaler
import java.nio.ByteBuffer
import io.opentelemetry.sdk.trace.data.SpanData

/** A class which wraps the shared logic/file access for different exporter channels. */
class OtlpExporterCommon(output_directory: java.io.File):
  private val version = System.currentTimeMillis()
  //  Open all our files
  // TODO - gracefully handle failures.
  private val (files, resourceOutput, scopeOutput, spanOutput) =
    val files = ArrayBuffer[RandomAccessFile]()
    val resourceOutput =
        val f = RandomAccessFile(new java.io.File(output_directory, "resource.otlp"), "rw")
        files.append(f)
        DictionaryOutputChannel(f.getChannel(), version, 512)
    val scopeOutput = 
        val f = RandomAccessFile(new java.io.File(output_directory, "scope.otlp"), "rw")
        files.append(f)
        DictionaryOutputChannel(f.getChannel(), version, 512)
    val spanOutput = 
        val f = RandomAccessFile(new java.io.File(output_directory, "spans.otlp"), "rw")
        files.append(f)
        RingBufferOutputChannel(f.getChannel(), version, 512, 100)
    (files, resourceOutput, scopeOutput, spanOutput)
  private val resourceDictionary = new java.util.concurrent.ConcurrentHashMap[Resource, Long]()
  // TODO - shrink this over time?
  private val scopeDictionary = new java.util.concurrent.ConcurrentHashMap[InstrumentationScopeInfo, Long]()

  // TOOD - Concurrency safe resource writes.
  private def writeResource(r: Resource, out: ByteBuffer): Unit = 
    val m = ResourceMarshaler.create(r)
    val bos = ByteBufferOutputStream(out)
    writeVarInt64(m.getBinarySerializedSize(), bos)
    m.writeBinaryTo(bos)
  // TOOD - Concurrency safe scope writes.
  private def writeInstrumentationScope(s: InstrumentationScopeInfo, out: ByteBuffer): Unit = 
    val m = InstrumentationScopeMarshaler.create(s)
    val bos = ByteBufferOutputStream(out)
    writeVarInt64(m.getBinarySerializedSize(), bos)
    m.writeBinaryTo(bos)
  private def writeSpanEntry(s: SpanData, resourceId: Long, spanId: Long, out: ByteBuffer): Unit =
    // We need write length first.
    val m = ResourceRefSpanMarshaler(s, resourceId, spanId)
    val bos = ByteBufferOutputStream(out)
    writeVarInt64(m.getBinarySerializedSize(), bos)
    m.writeBinaryTo(bos)

  // TODO - gracefully handle failures.
  final def close(): Unit = files.foreach(_.close())


  /** Writes a span to the OTLP mmap channel. */
  final def writeSpan(s: SpanData): Unit =
    val resource = resourceDictionary.computeIfAbsent(s.getResource, r => resourceOutput.writeEntry(buf => writeResource(r, buf)))
    val scope = scopeDictionary.computeIfAbsent(s.getInstrumentationScopeInfo(), s => scopeOutput.writeEntry(buf => writeInstrumentationScope(s, buf)))
    spanOutput.writeChunk(buf => writeSpanEntry(s, resource, scope, buf))


  // TODO - this level of protobuf hackery should be exposed internally for usage from OTLP serialziation code.
  private def writeVarInt64(value: Long, out: ByteBufferOutputStream): Unit = writeULongInline(value, b => out.write(b))
  private inline def writeULongInline(value: Long, inline writeByte: Byte => Unit): Unit =
    val length = varIntLength(value)
    var shiftedValue = value
    var i = 0
    while i < length do
      writeByte(((shiftedValue & 0x7F) | 0x80).toByte)
      shiftedValue >>>=7
      i += 1
    writeByte(shiftedValue.toByte)
  private val VarIntLengths = (for (i <- 0 to 64) yield (63-i)/7).toArray
  private def varIntLength(value: Long): Int = VarIntLengths(java.lang.Long.numberOfLeadingZeros(value))