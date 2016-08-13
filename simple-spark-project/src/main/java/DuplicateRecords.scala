import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.JdbcRDD
import java.sql.{Connection, DriverManager, ResultSet}

/**
 * @author ollopollo
 */
object DuplicateRecords {
 def main(args: Array[String]) {
 
       val url="jdbc:mysql://hdpserver.itversity.com/demo"
       val username = "demo_user"
       val password = "itversity"
       Class.forName("com.mysql.jdbc.Driver").newInstance
       
       val conf = new SparkConf().setAppName("Duplicate RDD").setMaster("local[2]").set("spark.executor.memory", "1g")
       val sc = new SparkContext(conf)
       
     //  val myRDD = new JdbcRDD( sc, () => DriverManager.getConnection(url,username,password) ,
    //  "select first_name,last_name,person_id from person where person_id > ? and person_id < ?",
   //   5, 9, 1, r => r.getString("person_id") + ", " + r.getString("first_name") + ", " + r.getString("last_name"))
       
    //   myRDD.foreach(println)
     //  myRDD.saveAsTextFile("src/test/resources/delta")
       
       val dataRDD = sc.textFile("src/test/resources/data").cache()
       
       val newRdd = sc.textFile("src/test/resources/delta")
       
       dataRDD.union(newRdd).map(rec => (rec.split(",")(0).toInt, rec)).reduceByKey((x,y)=> y).map(rec => rec._2).foreach {println }
  }
}