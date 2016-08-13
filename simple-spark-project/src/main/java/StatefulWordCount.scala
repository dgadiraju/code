import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._

object StatefulWordCount {


  def main(args: Array[String]) {

    def updateFunction(values: Seq[Int], runningCount: Option[Int]) = {
      val newCount = values.sum + runningCount.getOrElse(0)
      new Some(newCount)
    }
    val ssc = new StreamingContext("local[2]", "Statefulwordcount", Seconds(10))
    val lines = ssc.socketTextStream("192.168.56.101", 9999)
    ssc.checkpoint("C://NewDir")
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    
    val totalWordCount = wordCounts.updateStateByKey(updateFunction _)
    totalWordCount.print()
    ssc.start()
    ssc.awaitTermination()
  }


}