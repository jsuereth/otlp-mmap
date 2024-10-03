import io.opentelemetry.otlp.mmap.internal.RingBufferOutputChannel
import java.io.RandomAccessFile
import java.io.File

val EXPORT_META_FILE = new File("../../export.meta")

import io.opentelemetry.sdk.trace.`export`.SpanExporter
import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.trace.data.SpanData
import java.{util => ju}
import io.opentelemetry.otlp.mmap.internal.DictionaryOutputChannel
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.sdk.common.InstrumentationScopeInfo
import java.nio.ByteBuffer

// TODO - better resource management of OTLP interaction...
class OtlpMmapSpanExporter(output_directory: java.io.File)(implicit manager: scala.util.Using.Manager) extends SpanExporter:
  private val version = System.currentTimeMillis()
  private val resourceOutput = DictionaryOutputChannel(manager(manager(RandomAccessFile(new java.io.File(output_directory, "resource.otlp"), "rw")).getChannel()), version, 512)
  private val scopeOutput = DictionaryOutputChannel(manager(manager(RandomAccessFile(new java.io.File(output_directory, "scope.otlp"), "rw")).getChannel()), version, 512)
  private val spanOutput = RingBufferOutputChannel(manager(manager(RandomAccessFile(new java.io.File(output_directory, "spans.otlp"), "rw")).getChannel()), version, 512, 100)
  private val resourceDictionary = new java.util.concurrent.ConcurrentHashMap[Resource, Long]()
  // TODO - shrink this over time?
  private val scopeDictionary = new java.util.concurrent.ConcurrentHashMap[InstrumentationScopeInfo, Long]()

  private def writeResource(r: Resource, out: ByteBuffer): Unit = ???
  private def writeInstrumentationScope(r: InstrumentationScopeInfo, out: ByteBuffer): Unit = ???
  private def writeSpanEntry(s: SpanData, resourceId: Long, spanId: Long, out: ByteBuffer): Unit = ???
  override def `export`(spans: ju.Collection[SpanData]): CompletableResultCode =
    spans.forEach: span =>
      val resourceId = resourceDictionary.computeIfAbsent(span.getResource(), resource => resourceOutput.writeEntry(buf => writeResource(resource, buf)))
      val scopeId = scopeDictionary.computeIfAbsent(span.getInstrumentationScopeInfo(), scope => scopeOutput.writeEntry(buf => writeInstrumentationScope(scope, buf)))
      spanOutput.writeChunk: chunk =>
        // TODO - serialize a flat buffer or other type of file here...
        writeSpanEntry(span, resourceId, scopeId, chunk)
    CompletableResultCode.ofSuccess()
  override def flush(): CompletableResultCode =
    // TODO - should we force writing to disk?
    // For now we do not.  We allow the OS to control persistence, as this is meant to give us speed of shared-memory IPC with slightly increased resilience on process crash.
    // Forcing flush losing some benefits without gaining enough.
    CompletableResultCode.ofSuccess()
  override def shutdown(): CompletableResultCode =
    // TODO - clean up resources...
    ???


@main def hello(): Unit =
  scala.util.Using.Manager: use =>
    val file = use(RandomAccessFile(EXPORT_META_FILE, "rw"))
    val channel = use(file.getChannel())
    val my_channel = use(RingBufferOutputChannel(channel, System.currentTimeMillis(), 64, 100))

    //for i <- 1 until 100000 do
    for i <- 0 until 110 do
      System.out.println(s"Writing index: $i")
      try my_channel.writeChunk: buffer =>
        buffer.asCharBuffer().append(f"i:$i%06d")
      catch
        case t: Throwable =>
          t.printStackTrace()
          throw t
    System.out.println("Done!")
