import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.JdbcRDD
import java.sql.{Connection, DriverManager, ResultSet}

/**
 * @author ollopollo
 */
object JdbcRddExample {
 def main(args: Array[String]) {
 
       val url="jdbc:mysql://hdpserver.itversity.com/demo"
       val username = "demo_user"
       val password = "itversity"
       Class.forName("com.mysql.jdbc.Driver").newInstance
       
       val conf = new SparkConf().setAppName("JDBC RDD").setMaster("local[2]").set("spark.executor.memory", "1g")
       val sc = new SparkContext(conf)
       
       val myRDD = new JdbcRDD( sc, () => DriverManager.getConnection(url,username,password) ,
      "select first_name,last_name,gender from person limit ?, ?",
      0, 5, 1, r => r.getString("last_name") + ", " + r.getString("first_name"))
       
       myRDD.foreach(println)
       myRDD.saveAsTextFile("C:\\jdbcrddexample")
       
  }
}