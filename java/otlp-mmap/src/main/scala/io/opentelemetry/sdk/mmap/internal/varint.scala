package io.opentelemetry.sdk.mmap.internal

import java.io.OutputStream

object VarInt:
  /** Helper to write varints compatible with Proto files. */
  def writeVarInt64(value: Long, out: OutputStream): Unit = writeULongInline(value, b => out.write(b))
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
  /** Length in bytes of a given variable-sized integer. */
  def varIntLength(value: Long): Int = VarIntLengths(java.lang.Long.numberOfLeadingZeros(value))
