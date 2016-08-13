
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext._

/**
 * @author ollopollo
 */
object SimpleSparkSqlExample {
  
    def main(args : Array[String]){
  
  val conf = new SparkConf().setAppName("SimpleSparkSQL Application").setMaster("local[2]").set("spark.executor.memory", "1g")
  val sc = new SparkContext(conf)
  
  val sqlc = new org.apache.spark.sql.SQLContext(sc)//sqlsc
  
  val p=sc.textFile("src/test/resources/Person")
  import sqlc.implicits._
  val pmap = p.map(p=> p.split(","))
  val PersonRdd=pmap.map(p=>People(p(0),p(1),p(2).toInt))
  
  
  
  val PersonDF = PersonRdd.toDF //RDD+Schema
  PersonDF.registerTempTable("Person")
  
  print(sqlc.sql("select count(*) from Person").collect())
  
    }  
}