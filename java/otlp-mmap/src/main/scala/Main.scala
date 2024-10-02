import io.opentelemetry.otlp.mmap.internal.RingBufferOutputChannel
import java.io.RandomAccessFile
import java.io.File

val EXPORT_META_FILE = new File("../../export.meta")

  

@main def hello(): Unit =
  scala.util.Using.Manager: use =>
    val file = use(RandomAccessFile(EXPORT_META_FILE, "rw"))
    val channel = use(file.getChannel())
    val my_channel = use(RingBufferOutputChannel(channel, 64, 100))

    //for i <- 1 until 100000 do
    for i <- 0 until 110 do
      System.out.println(s"Writing index: $i")
      try my_channel.writeChunk: buffer =>
        buffer.asCharBuffer().append(f"i:$i%06d")
      catch
        case t: Throwable =>
          t.printStackTrace()
          throw t
    System.out.println("Done!")
