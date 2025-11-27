package io.opentelemetry.sdk.mmap.internal

import java.time.Instant
import io.opentelemetry.context.Context
import com.google.protobuf.ByteString
import opentelemetry.proto.mmap.v1.Mmap.SpanContext

/**
 * Calculate the log base 2 of the supplied integer, essentially reports the location
 * of the highest bit.
 *
 * @param value Positive value to calculate log2 for.
 * @return The log2 value
 */
inline def log2(value: Int): Int = 
    // TODO - error checking.
    Integer.SIZE - Integer.numberOfLeadingZeros(value) - 1

/**
  * Calculates the minimum power of two that can encapsulate a size.
  *
  * For example, the minimum power of two to handle 5 would be 8
  */
inline def minPowOfTwo(size: Int): Int =
    1 << (Integer.SIZE - Integer.numberOfLeadingZeros(size - 1))


def convertInstant(instant: Instant): Long =
    (instant.getEpochSecond() * 1_000_000_000L) + instant.getNano()

def convertContext(context: Context) =
    convertSpanContext(io.opentelemetry.api.trace.Span.fromContext(context).getSpanContext)

def convertSpanContext(sc: io.opentelemetry.api.trace.SpanContext) =
    val b = opentelemetry.proto.mmap.v1.Mmap.SpanContext.newBuilder()
    // TODO - flags here is wrong, need to fix.
    b.setFlags(sc.getTraceFlags().asByte())
    b.setSpanId(ByteString.copyFrom(sc.getSpanIdBytes()))
    b.setTraceId(ByteString.copyFrom(sc.getTraceIdBytes()))
    b.build()