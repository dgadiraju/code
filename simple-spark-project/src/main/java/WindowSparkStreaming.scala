import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.StreamingContext._

object WindowSparkStreaming {

  def main(args: Array[String]) {

     val ssc = new StreamingContext("local[2]", "Statefulwordcount", Seconds(10))

    val lines = ssc.socketTextStream("192.168.56.101", 9999)

    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    pairs.reduceByKeyAndWindow((x: Int, y: Int) => (x + y), Seconds(30), Seconds(10)).print()

    ssc.start()
    ssc.awaitTermination()
  }

}