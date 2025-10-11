package io.opentelemetry.sdk.mmap.internal

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
