package io.opentelemetry.sdk.mmap.internal

import java.lang.foreign.ValueLayout
import java.lang.foreign.MemorySegment

object Header:
    val longMetaHandle = ValueLayout.JAVA_LONG.arrayElementVarHandle()
    val intMetaHandle = ValueLayout.JAVA_INT.arrayElementVarHandle()

trait Header:
  def segment: MemorySegment
  def force(): Unit = segment.force()
  // TODO - fix this so it's not treating index as an index into a large long array, but
  // instead a full byte offset to a long we use.
  final class MetadataLongField(offset: Long):
    inline def get(): Long = Header.longMetaHandle.get(segment, offset, 0)
    inline def getVolatile(): Long = Header.longMetaHandle.getVolatile(segment, offset, 0)
    inline def set(value: Long) = Header.longMetaHandle.set(segment, offset, 0, value)
    inline def setVolatile(value: Long) = Header.longMetaHandle.setVolatile(segment, offset, 0, value)
    inline def setRelease(value: Long) = Header.longMetaHandle.setRelease(segment, offset, 0, value)
    inline def compareAndSet(expected: Long, value: Long): Boolean = Header.longMetaHandle.compareAndSet(segment, offset, 0, expected, value)
    inline def getAndAdd(value: Long): Long = Header.longMetaHandle.getAndAdd(segment, offset, 0, value)

  /** An integer array. */
  final class MetadataIntArray(offset: Int, size: Int):
    // TODO - what do we need?
    /** Returns the minimum value in this array. */
    inline def min(): Int =
        (0 until length).map(apply).fold(Integer.MAX_VALUE)(Math.min)
    inline def apply(idx: Int): Int =
        // TODO - validate length
        Header.intMetaHandle.getAcquire(segment, offset, idx)
    // We treat assignment as "set release".
    inline def update(idx: Int, value: Int): Unit =
        Header.intMetaHandle.setRelease(segment, offset, idx, value)
    def length: Int = size
