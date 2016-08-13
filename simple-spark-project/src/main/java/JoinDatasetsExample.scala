

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.hadoop.fs.shell.Count
import java.io.Reader
import java.io.StringReader
import au.com.bytecode.opencsv.CSVParser
import au.com.bytecode.opencsv.CSVReader
/**
 * @author ollopollo
 */
object JoinDatasetsExample {
   def main(args : Array[String]){
    val conf = new SparkConf().setAppName("Nyse MapReduce Application").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
  
  val nyseData=sc.textFile("src/test/resources/nyse_master.txt")
  
  val nyseHeader=sc.textFile("src/test/resources/companylist_header.csv")

    val nyseRDD=nyseData.map (row=>{
      val coloumns = row.split(",")
      (coloumns(0),coloumns(6))
    })
    
     val nyseHeadRDD = nyseHeader.mapPartitions(lines => {
         val parser=new CSVParser('|')
         lines.map(line => {
           val columns = parser.parseLine(line)
           (columns(0),columns(6))
         })
       })
 
       
       val joinRDD = nyseHeadRDD.join(nyseRDD).filter(x=> {
         val vol =x._2._2.toInt
         vol > 1000;
       })
       joinRDD.foreach(println)
      
   }
}