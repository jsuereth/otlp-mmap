package io.opentelemetry.sdk.mmap.internal

import java.io.OutputStream
import java.nio.ByteBuffer

object VarInt:
  /** Helper to write varints compatible with Proto files. */
  def writeVarInt64(value: Long, out: OutputStream): Unit = writeULongInline(value, b => out.write(b))
  /** Helper to read varints compatible with Proto files. */
  def readVarInt64(in: ByteBuffer): Long = readULongInline(() => in.get())
  /** Length in bytes of a given variable-sized integer. */
  def sizeVarInt64(value: Long): Int = varIntLength(value) + 1
  private inline def readULongInline(inline readNext: () => Byte): Long =
    var currentByte: Byte = readNext()
    if (currentByte & 0x80) == 0 then currentByte.toLong
    else
      var result: Long = currentByte & 0x7f
      var offset = 0
      while
        offset += 7
        currentByte = readNext()
        result |= (currentByte & 0x7F).toLong << offset
        (currentByte & 0x80) != 0 // && offset < 64 
      do ()
      result
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
  private def varIntLength(value: Long): Int = VarIntLengths(java.lang.Long.numberOfLeadingZeros(value))

