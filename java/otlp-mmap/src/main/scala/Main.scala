import java.io.IOException
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.io.File
import sun.misc.Unsafe;
import java.nio.ByteOrder
import java.util.zip.CRC32
import java.nio.MappedByteBuffer
import java.lang.foreign.Arena
import java.nio.channels.FileChannel.MapMode
import java.lang.foreign.MemorySegment
import java.lang.foreign.ValueLayout
import java.io.Closeable
import scala.collection.mutable.ArrayBuffer

val EXPORT_META_FILE = new File("../../export.meta")
val EXPORT_VERISON = 317L

// def getUnsafe: Unsafe =
//   val f = classOf[Unsafe].getDeclaredField("theUnsafe")
//   f.setAccessible(true);
//   f.get(null).asInstanceOf[Unsafe]

object Metadata:
  val simpleHandle = ValueLayout.JAVA_LONG.varHandle()
  val metaHandle = ValueLayout.JAVA_LONG.arrayElementVarHandle()
  val VERSION_INDEX = 0
  val LENGTH_INDEX = 1
  val CHUNK_SIZE_INDEX = 2
  val CHECKSUM_INDEX = 5
  val READ_POSITION_INDEX = 6
  val WRITE_POSITION_INDEX = 7


// A helper that allows us to semantically write to a memory segment when needed.
class SharedMetadata(segment: MemorySegment):
  // Helper class to simplify doing direct memory access using concurrency primitives.
  class MetadataLongField(index: Int):
    inline def get(): Long = Metadata.metaHandle.get(segment, index)
    inline def getVolate(): Long = Metadata.metaHandle.getVolatile(segment, index)
    inline def set(value: Long) = Metadata.metaHandle.set(segment, index, value)
    inline def setVolatile(value: Long) = Metadata.metaHandle.setVolatile(segment, index, value)
    inline def compareAndSet(expected: Long, value: Long): Boolean = Metadata.metaHandle.compareAndSet(segment, index, expected, value)
  val version = MetadataLongField(Metadata.VERSION_INDEX)
  val length = MetadataLongField(Metadata.LENGTH_INDEX)
  val chunkSize = MetadataLongField(Metadata.CHUNK_SIZE_INDEX)
  val checksum = MetadataLongField(Metadata.CHECKSUM_INDEX)
  val readPosition = MetadataLongField(Metadata.READ_POSITION_INDEX)
  val writePosition = MetadataLongField(Metadata.WRITE_POSITION_INDEX)
  inline def force(): Unit = segment.force()

class OutputChannel(channel: FileChannel, chunk_length: Long, num_chunks: Long) extends Closeable:
  val version = System.currentTimeMillis()
  val arena = Arena.ofConfined()
  val metadata = SharedMetadata(channel.map(MapMode.READ_WRITE, 0, 64, arena))
  private var currentIndex = 1
  private var chunks: collection.mutable.ArrayBuffer[MemorySegment] =
    (1 to (num_chunks+1).toInt).map: i =>
      channel.map(MapMode.READ_WRITE, chunk_length*i, chunk_length*i+1, arena)
    .to(ArrayBuffer)
  writeHeader()

  private def currentChunk: MemorySegment =
    chunks(currentIndex)

  private def moveNextChunk(): Unit =
    // Note this will block until we can write.
    def tryMoveNextChunk(): Boolean =
      val end = metadata.readPosition.getVolate()
      val current = metadata.writePosition.get()
      val next = (current + 1) % (num_chunks+1)
      if (next != end) && metadata.writePosition.compareAndSet(current, next)
      then 
        currentIndex = next.toInt
        true
      else false
    // TODO - exponential backoff
    while !tryMoveNextChunk()
    do Thread.`yield`()

  private def writeHeader(): Unit =
    try
      println(s"Writing version: ${version}")
      metadata.version.set(version)
      println(s"Writing chunk size ${chunk_length}")
      metadata.chunkSize.set(chunk_length)
      metadata.length.set(num_chunks)
      metadata.readPosition.set(0)
      metadata.writePosition.set(0)
      println(s"Reading version: ${metadata.version.get()}")
      println(s"Reading chunk size: ${metadata.chunkSize.get()}")
      println(s"Reading length size: ${metadata.length.get()}")
    catch
      case t: Throwable =>
        t.printStackTrace()
        throw t
    
  def writeChunk[A](writer: ByteBuffer => A): A =
    try writer(currentChunk.asByteBuffer().order(ByteOrder.nativeOrder()))
    finally
      System.out.println(s"Wrote index: $currentIndex, moving to next index")
      moveNextChunk()

  // TODO - allocate ring-buffer.

  // TODO - create next chunk method that will ensure we have a bytebufer at the right piece of memory.

  def close(): Unit =
    arena.close()
    channel.force(true)
  

@main def hello(): Unit =
  scala.util.Using.Manager: use =>
    val file = use(RandomAccessFile(EXPORT_META_FILE, "rw"))
    val channel = use(file.getChannel())
    val my_channel = use(OutputChannel(channel, 64, 100))

    for i <- 1 until 100000 do
      System.out.println(s"Writing index: $i")
      try my_channel.writeChunk: buffer =>
        buffer.asCharBuffer().append(f"i:$i%06d")
      catch
        case t: Throwable =>
          t.printStackTrace()
          throw t
    System.out.println("Done!")
