package io.opentelemetry.sdk.internal

import io.opentelemetry.api.trace.SpanId
import io.opentelemetry.api.trace.TraceId
import java.util.concurrent.ThreadLocalRandom
import java.util.function.Supplier

/** Generates ids for traces. */
trait IdGenerator:
    /** Generates a span id. */
    def generateSpanId(): String
    /** Generates a trace id. */
    def generateTraceId(): String

object IdGenerator:
    def random(): IdGenerator =
        RandomIdGenerator(() => ThreadLocalRandom.current)

class RandomIdGenerator(random: Supplier[java.util.Random]) extends IdGenerator:
  override def generateSpanId(): String = 
    var id = 0L
    // 0 is an invalid id
    while id == 0
    do id = random.get().nextLong()
    SpanId.fromLong(id)
  override def generateTraceId(): String =
    // Trace allows 0 as valid high bits.
    val idHi = random.get().nextLong()
    var idLo = 0L
    while idLo == 0
    do idLo = random.get().nextLong()
    TraceId.fromLongs(idHi, idLo)


