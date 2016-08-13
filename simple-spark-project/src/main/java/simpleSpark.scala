
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
/**
 * @author ollopollo
 */
object simpleSpark {
  def main(args : Array[String]){
    //println("hi1")
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    val myFile = sc.textFile("C:\\streaming\\newFile.txt")
    val wordspair =myFile.flatMap(row =>row.split(" ")).map(x=>(x,1)).reduceByKey(_+_)
    wordspair.foreach(println)
    
  }
}