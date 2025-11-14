import java.io.File
import io.opentelemetry.otlp.mmap.OtlpMmapExporter
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk
import io.opentelemetry.sdk.trace.`export`.SimpleSpanProcessor
import io.opentelemetry.sdk.trace.`export`.SpanExporter
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter
import io.opentelemetry.sdk.mmap.internal.SdkMmap
import io.opentelemetry.api.OpenTelemetry
import StartupChoice.OtelSdk
import io.opentelemetry.sdk.mmap.MiniOpenTelemetry
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.sdk.mmap.internal.SdkMmapRaw
import java.io.RandomAccessFile
import io.opentelemetry.sdk.mmap.internal.SdkMmapOptions
import io.opentelemetry.sdk.mmap.internal.RingBufferOptions
import java.io.PrintWriter
import java.util.UUID

val EXPORT_META_DIRECTORY = new File("../../export")

@main def demo(): Unit =
  val http_endpoint = sys.env.get("HTTP_ENDPOINT_PORT").map(port => port.toInt)
  val mmap_export = sys.env.get("OTLP_MMAP_EXPORTER_DIRECTORY").map(dir => new java.io.File(dir))
  val otlp_export = sys.env.get("OTEL_EXPORTER_OTLP_ENDPOINT")
  val mmap_sdk = sys.env.get("SDK_MMAP_EXPORTER_FILE").map(f => new java.io.File(f))
  val otel = (mmap_export, mmap_sdk, otlp_export) match
    case (Some(mmap_dir), _, _) => initOtel(StartupChoice.OtelSdk(OtlpMmapExporter(mmap_dir).spanExporter))
    case (None, Some(mmap_file), _) =>
      // Kill the file if it exists or otherwise wipe it, until we sort out retry  / different loads.
      if mmap_file.exists()
      then
        mmap_file.delete()
        mmap_file.createNewFile()
      initOtel(StartupChoice.MmapSdk(SdkMmapRaw(new RandomAccessFile(mmap_file, "rw"), SdkMmapOptions(
        events = RingBufferOptions(512,64),
        measurements = RingBufferOptions(512,64),
        spans = RingBufferOptions(512,64),
      ))))
    case (None, _, Some(otlp_endpoint)) => initOtel(StartupChoice.NormalOtel(otlp_endpoint))
    case _ => initOtel(StartupChoice.OtelSdk(OtlpMmapExporter(EXPORT_META_DIRECTORY).spanExporter))
  // TODO - metrics.
  http_endpoint match
    case Some(endpoint) => runHttpServer(endpoint, otel)
    case None => makeSpans(otel)
  

enum StartupChoice:
  case NormalOtel(endpoint: String)
  case MmapSdk(mmap: SdkMmapRaw)
  case OtelSdk(exporter: SpanExporter)



def initOtel(choice: StartupChoice): OpenTelemetry =
  choice match
    // TODO - This configures a batch export.
    // OTLP-MMAP does synchronous export - however given batching is the default, we need to compare our
    // performance against it.
    case StartupChoice.NormalOtel(endpoint) => 
      AutoConfiguredOpenTelemetrySdk
      .builder()
      .addPropertiesSupplier(() => java.util.Map.of(
        "otel.traces.exporter", "otlp", 
        "otel.metrics.exporter", "otlp",
        "otel.logs.exporter", "otlp",
        "otel.exporter.otlp.endpoint", endpoint,
      ))
      .addResourceCustomizer((r,prop) => {
        r.toBuilder()
        .put("service.instance.id", UUID.randomUUID().toString())
        .build()
      })
      .build().getOpenTelemetrySdk()
    case StartupChoice.OtelSdk(exporter) =>
        AutoConfiguredOpenTelemetrySdk.builder()
        .addPropertiesSupplier(() => java.util.Map.of(
          "otel.traces.exporter", "none",
          "otel.metrics.exporter", "none",
          "otel.logs.exporter", "none"))
        .addTracerProviderCustomizer((tracer, config) =>
          tracer.addSpanProcessor(
            SimpleSpanProcessor.builder(exporter)
            .build()
          ))
        .setResultAsGlobal()
        .build().getOpenTelemetrySdk()
    case StartupChoice.MmapSdk(mmap) =>
      val otel = MiniOpenTelemetry(mmap)
      GlobalOpenTelemetry.set(otel)
      otel


def makeSpans(otel: OpenTelemetry): Unit =
  val tracer = otel.getTracer("test-spans")
  for i <- 0 until 200
  do 
    System.out.println(s"-- Generating span: $i --")
    val s = 
      tracer.spanBuilder(s"span $i")
      .setAttribute("test.int", 1)
      .setAttribute("test.string", "longer")
      .startSpan()
    try s.addEvent("test")
    finally s.end()

    
// TODO - use an HTTP server that will generate spans.
def runHttpServer(endpoint: Int, otel: OpenTelemetry): Unit =
  println(s"Starting server on ${endpoint}")
  Util.startJvmMetrics(otel)
  Util.startHttpServer(otel, endpoint)
  // TOOD - should we wait for server to stop?
  ()