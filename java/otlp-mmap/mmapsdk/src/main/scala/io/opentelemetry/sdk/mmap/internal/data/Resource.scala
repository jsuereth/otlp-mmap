package io.opentelemetry.sdk.mmap.internal
package data

import io.opentelemetry.api.common.Attributes
import opentelemetry.proto.mmap.v1.{Mmap=>MmapProto}
import java.nio.ByteBuffer

/** An OTLP resource.  TODO - flesh this out. */
final case class Resource(attributes: Attributes)
