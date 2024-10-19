import java.io.File
import io.opentelemetry.otlp.mmap.OtlpMmapExporter
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk
import io.opentelemetry.sdk.trace.`export`.SimpleSpanProcessor
import io.opentelemetry.sdk.trace.`export`.SpanExporter
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter

val EXPORT_META_DIRECTORY = new File("../../export")

@main def demo(): Unit =
  val mmap_export = sys.env.get("OTLP_MMAP_EXPORTER_DIRECTORY").map(dir => new java.io.File(dir))
  val otlp_export = sys.env.get("OTEL_EXPORTER_OTLP_ENDPOINT")
  (mmap_export, otlp_export) match
    case (Some(mmap_dir), _) => makeSpans(OtlpMmapExporter(mmap_dir).spanExporter)
    case (None, Some(otlp_endpoint)) => makeSpans(OtlpHttpSpanExporter.builder().setEndpoint(otlp_endpoint).build())
    case _ => makeSpans(OtlpMmapExporter(EXPORT_META_DIRECTORY).spanExporter)


def makeSpans(exporter: SpanExporter): Unit =
  val otel =
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
    .build()

  val tracer = otel.getOpenTelemetrySdk().getTracer("test-spans")
  for i <- 0 until 200
  do 
    System.out.println(s"-- Generating span: $i --")
    val s = tracer.spanBuilder(s"span $i").startSpan()
    try ()
    finally s.end()

    
  
