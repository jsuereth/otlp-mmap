import io.opentelemetry.api.OpenTelemetry
import java.io.PrintWriter

object Util:
    /** Starts JVM JFR metric collection. */
    def startJvmMetrics(otel: OpenTelemetry): Unit =
        import io.opentelemetry.instrumentation.runtimemetrics.java17.*
        RuntimeMetrics.builder(otel)
            .disableAllJmx()
            .enableFeature(JfrFeature.CLASS_LOAD_METRICS)
            .enableFeature(JfrFeature.CPU_COUNT_METRICS)
            .enableFeature(JfrFeature.GC_DURATION_METRICS)
            .enableFeature(JfrFeature.LOCK_METRICS)
            .enableFeature(JfrFeature.CONTEXT_SWITCH_METRICS)
            .enableFeature(JfrFeature.MEMORY_ALLOCATION_METRICS)
            .enableFeature(JfrFeature.MEMORY_POOL_METRICS)
            .enableFeature(JfrFeature.NETWORK_IO_METRICS)
            .enableFeature(JfrFeature.THREAD_METRICS)
            .build()
    /** Starts an http server on the given port. */
    def startHttpServer(otel: OpenTelemetry, endpoint: Int): com.sun.net.httpserver.HttpServer =
        import com.sun.net.httpserver.{HttpContext, HttpServer}
        import java.net.InetSocketAddress
        import io.opentelemetry.instrumentation.javahttpserver.JavaHttpServerTelemetry
        val server = HttpServer.create(new InetSocketAddress(endpoint), 0)
        val context = server.createContext(
                    "/",
                    ctx => {
                        // TODO - Update this if needed.
                        val body = "Hello"
                        ctx.sendResponseHeaders(200, body.length())
                        // otel.getTracer("test").spanBuilder("test").startSpan().end()
                        val out = new PrintWriter(ctx.getResponseBody())
                        out.print(body)
                        out.close()
                    });
        JavaHttpServerTelemetry.create(otel).configure(context)
        server.start()
        server