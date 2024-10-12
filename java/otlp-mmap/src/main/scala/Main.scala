import java.io.File
import io.opentelemetry.otlp.mmap.OtlpMmapExporter
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk
import io.opentelemetry.sdk.trace.`export`.SimpleSpanProcessor


val EXPORT_META_DIRECTORY = new File("../../export")

@main def makeSpans(): Unit =
  val mmap = OtlpMmapExporter(EXPORT_META_DIRECTORY)
  val otel =
    AutoConfiguredOpenTelemetrySdk.builder()
    .addPropertiesSupplier(() => java.util.Map.of(
      "otel.traces.exporter", "none",
      "otel.metrics.exporter", "none",
      "otel.logs.exporter", "none"))
    .addTracerProviderCustomizer((tracer, config) =>
      tracer.addSpanProcessor(
        SimpleSpanProcessor.builder(mmap.spanExporter)
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

    
  
