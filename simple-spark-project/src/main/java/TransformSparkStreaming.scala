import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._

object TransformSparkStreaming {


  def main(args: Array[String]) {

  
    val ssc = new StreamingContext("local[2]", "Statefulwordcount", Seconds(10))
    
    val myFile = ssc.sparkContext.textFile("C:\\streaming\\newFile.txt")
    val wordspair =myFile.flatMap(row =>row.split(" ")).map(x=>(x,1)).reduceByKey(_+_)
      
    val oldwordCount=wordspair.reduceByKey(_+_)
    
    val lines = ssc.socketTextStream("192.168.56.101", 9999)
    
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    
    val joinRDD =  wordCounts.transform(rdd=>{
      rdd.join(wordspair).map{
        case(word,(oldCount,newCount))=>{
          (word,oldCount+newCount)
        }
      }
    })
    joinRDD.print()
    ssc.start()
    ssc.awaitTermination()
  }


}