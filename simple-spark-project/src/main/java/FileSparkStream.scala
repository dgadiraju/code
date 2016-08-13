

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
object FileSparkStream {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("MyfirstStreamingAp").set("spark.executor.memory", "1g")
    val ssc = new StreamingContext(conf, Seconds(10))
    val lines = ssc.textFileStream("C:\\filedemo")
    lines.flatMap (x => x.split(" ") ).map ( x => (x,1)).reduceByKey(_+_).print()
    ssc.start()
    ssc.awaitTermination()
  }

}