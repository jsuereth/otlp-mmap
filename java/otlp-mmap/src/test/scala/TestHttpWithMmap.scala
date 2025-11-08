import munit.FunSuite
import io.opentelemetry.sdk.mmap.internal.SdkMmapOptions
import io.opentelemetry.sdk.mmap.internal.RingBufferOptions
import java.io.RandomAccessFile
import io.opentelemetry.sdk.mmap.internal.SdkMmapRaw
import io.opentelemetry.sdk.mmap.MiniOpenTelemetry
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.URI
import java.net.http.HttpResponse
import io.opentelemetry.sdk.mmap.internal.data.ProtoReader

class TestHttpWithMmap extends FunSuite:
    import opentelemetry.proto.mmap.v1.{Mmap=>MmapProto}
    given io.opentelemetry.sdk.mmap.internal.Readable[MmapProto.SpanEvent] = ProtoReader(MmapProto.SpanEvent.getDefaultInstance())
    def httpGet(url: String): HttpResponse[String] = 
        val client = HttpClient.newHttpClient()

        // 2. Build an HttpRequest
        val request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .build()
        client.send(request,HttpResponse.BodyHandlers.ofString())

    test("basic http server") {
        val opts = SdkMmapOptions(
            events = RingBufferOptions(512,16),
            measurements = RingBufferOptions(2048,16),
            spans = RingBufferOptions(2048,128)
        )
        val file = java.io.File.createTempFile("mmap", "otlp")
        file.deleteOnExit()
        val raf = new RandomAccessFile(file, "rw")
        val mmap = SdkMmapRaw(raf, opts)
        val otel = MiniOpenTelemetry(mmap)

        // Start HTTP server
        val server = Util.startHttpServer(otel, 9091)
        try
            // Now do our test.
            val response = httpGet("http://localhost:9091")
            assertEquals(response.statusCode(), 200)

            // Now check for spans.
            assertEquals(true, mmap.spans.hasEvents())
            val start_event = mmap.spans.readNextBuffer()
            assertEquals(start_event.getStart().getName, "GET /")
            // TODO - read all intermediate events.
            while mmap.spans.hasEvents()
            do
                val event = mmap.spans.readNextBuffer()
                if event.hasEnd()
                then
                    assert(event.getEnd().getEndTimeUnixNano() != 0, s"End span does not include timestamp: ${event}")
        finally server.stop(0)
    }
