
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object NyseMapReduce {
  def main(args : Array[String]){
    val conf = new SparkConf().setAppName("Nyse MapReduce Application").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    val nyseData = sc.textFile(args(0))
 
    val nyseRDD=nyseData.map (row=>{
      val coloumns = row.split(",")
      (coloumns(0),coloumns(3))
    }).reduceByKey((x,y)=> if(x>y) x else y)
    

    
    
    
  val nyserdd =nyseData.map(row => row.split(",")).filter ( line => line(1).equals("hello") ).collect().toList
    /* map (line => (line(0),line(3))).
     reduceByKey((x,y)=>if(x>y) x else y).collect().foreach(println)*/
  }
}