import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.hive._
import java.sql.{Connection, DriverManager, ResultSet}

/**
 * @author ollopollo
 */
object HiveIncrementalExample {
 def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Duplicate RDD").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
   
    import org.apache.spark.storage.StorageLevel._
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val NewDF = hiveContext.load("jdbc",  
      Map(
      "driver" -> "com.mysql.jdbc.Driver",
      "url" -> "jdbc:mysql://hdpserver.itversity.com/demo",
      "user" -> "demo_user",
      "password" -> "itversity",
      
       /* "url" -> "jdbc:mysql://hdpserver.itversity.com/demo;user=demo_user;password=itversity",*/

      "dbtable" -> "person"))
      
      NewDF.show()
 }
}