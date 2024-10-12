package io.opentelemetry.otlp.mmap

import java.io.File
import java.{util=>ju}
import io.opentelemetry.otlp.mmap.internal.OtlpExporterCommon
import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.trace.`export`.SpanExporter
import io.opentelemetry.sdk.trace.data.SpanData

/** Helper which construts all OTLP exporters from a central place. */
class OtlpMmapExporter(outputDir: File):
    if !outputDir.exists() 
    then outputDir.mkdirs()
    private val common = OtlpExporterCommon(outputDir)
    val spanExporter = new SpanExporter:
        override def `export`(spans: ju.Collection[SpanData]): CompletableResultCode =
            spans.forEach(common.writeSpan)
            CompletableResultCode.ofSuccess()
        override def flush(): CompletableResultCode =
            // TODO - should we force writing to disk?
            // For now we do not.  We allow the OS to control persistence, as this is meant to give us speed of shared-memory IPC with slightly increased resilience on process crash.
            // Forcing flush losing some benefits without gaining enough.
            CompletableResultCode.ofSuccess()
        override def shutdown(): CompletableResultCode =
            // TODO - clean up resources...
            // TODO - Closing shoudl close down ALL exporters...
            try 
                common.close()
                CompletableResultCode.ofSuccess()
            catch
                case e: Exception => CompletableResultCode.ofFailure()



