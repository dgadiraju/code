import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.TypedColumn
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import com.databricks.spark.csv
/**
 * @author ollopollo
 */
object SparkSqlCsvJsonExample {
  
  def main(args : Array[String]){
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new org.apache.spark.SparkContext(conf)
    val sqlc = new org.apache.spark.sql.hive.HiveContext(sc)
       
    val PersonDF = sqlc.read.json("src/test/resources/Person.json")
    PersonDF.registerTempTable("Person")
    sqlc.cacheTable("Person")
    PersonDF.cache()
    val jdbc = sqlc.read.format("jdbc").options(
        Map(
      "driver" -> "com.mysql.jdbc.Driver",
      "url" -> "jdbc:mysql://hdpserver.itversity.com/demo",
      "user" -> "demo_user",
      "password" -> "itversity",
      "dbtable" -> "person"))
    
   // sqlc.sql("create table if not exists abc (key Int,value String) LoadData INPATH")
    /*PersonDF.coalesce(1).select("first_name", "age").write.mode("SaveMode.Override").format("com.databricks.spark.csv").save("src/test/resources/csv/")
    
   val agedf = sqlc.sql("select age from parquet.`src/test/resources/Person.parquet`")
   agedf.show()
   
   */
    
 //   PersonDF.saveAsTable("myschema.table","SaveMode.Append")
    
   /* val PersonDF= sqlc.read.format("json").load("src/test/resources/Person.json")
    
    PersonDF.printSchema()
    PersonDF.registerTempTable("Person")
    
    sqlc.sql("select * from Person where age < 60").collect().foreach (print)*/
  /*  
  val person=sc.textFile("src/test/resources/Person").map(rec => rec.split(",")).map(p => People(p(0),p(1),p(2).toInt))
  
  person.map (p=>p.age < 100)
    import sqlc.implicits._
    val personDF = person.toDF()
    
     
    personDF.registerTempTable("person")
    val firstName = sqlc.sql("select first_name from person where age < 60")
   // personDF.filter("salary < 10000").collect().foreach { x => println(x) }
    
    val personDS : Dataset[People] = personDF.as[People]
    
    val ds = Seq(People("Andy","Driver", 32)).toDS()
    
    val histogram = personDS.groupBy(_.first_name).mapGroups({
      case(name,person)=>{
        val bucket = new Array[Int](10)
        person.map(_.age).foreach { a  => 
          bucket(a/10)+=1}
        (name,bucket)
      }
            
    })
    personDS.select(expr("age").as[Int]).filter(p => p.toInt <15).show()*/
    
    
  //  histogram.collect().foreach(x=>print(x._1+" "+x._2.apply(1)))
    
    //firstName.collect().foreach { x => println(x) }
    
    /*val df = sqlc.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load("src/test/resources/Person.csv")
    
   df.map(t => "First Name: " + t(0)).collect().foreach(println)*/
    
  //  df.collect().foreach { x => println(x) }
  // df.select(col, cols)
    
    
    
    
  }
  
}