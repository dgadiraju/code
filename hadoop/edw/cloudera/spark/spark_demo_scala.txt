#Introduction
#spark-shell
#pyspark
#SparkContext
#SQLContext
#HiveContext
#spark-sql (only latest version)
#JDBC
#To connect to remote database using jdbc
#It works only from spark 1.3.0 or later
#Either you need to run spark shell with driver-class-path or set environment variable with os.environ

spark-shell --driver-class-path /usr/share/java/mysql-connector-java.jar
import org.apache.spark.sql.SQLContext
val sqlContext = new SQLContext(sc)
val url = "jdbc:mysql://quickstart.cloudera:3306/retail_db?user=retail_dba&password=cloudera"
sqlContext.load("jdbc", Map(
  "url" -> url,
  "dbtable" -> "departments")).collect().foreach(println)

##############################################################################
#Install sbt
#Download the tar file of sbt, untar and set environment variable SBT_HOME
#Update PATH

#Developing simple scala based applications for spark
#Save this to a file with scala extension
#Compile into jar file
cd
mkdir scala
cd scala; mkdir -p src/main/scala

#Copy below 4 lines into simple.sbt
name := "Simple Project"
version := "1.0"
scalaVersion := "2.10.4"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.1"

#Copy below 9 lines into src/main/scala/SimpleApp.scala
import org.apache.spark.SparkContext, org.apache.spark.SparkConf
object SimpleApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("scala spark")
    val sc = new SparkContext(conf)
    val dataRDD = sc.textFile("/user/cloudera/sqoop_import/departments")
    dataRDD.saveAsTextFile("/user/cloudera/scalaspark/departmentsTesting")
  }
}

#Build package
sbt package

#Run using this command
#master local will run in spark local mode
spark-submit --class "SimpleApp" --master local /home/cloudera/scala/target/scala-2.10/simple-project_2.10-1.0.jar

#master yarn will run in yarn mode
spark-submit --class "SimpleApp" --master yarn /home/cloudera/scala/target/scala-2.10/simple-project_2.10-1.0.jar

#Validate
hadoop fs -ls /user/cloudera/scalaspark/departmentsTesting

##############################################################################

# Load data from HDFS and storing results back to HDFS using Spark
import org.apache.spark.SparkContext

val dataRDD = sc.textFile("/user/cloudera/sqoop_import/departments")
dataRDD.collect().foreach(println)

dataRDD.count()

dataRDD.saveAsTextFile("/user/cloudera/scalaspark/departments")

#Object files are not available in python
dataRDD.saveAsObjectFile("/user/cloudera/scalaspark/departmentsObject")

#saveAsSequenceFile
import org.apache.hadoop.io._
dataRDD.map(x => (NullWritable.get(), x)).saveAsSequenceFile("/user/cloudera/scalaspark/departmentsSeq")
dataRDD.map(x => (x.split(",")(0), x.split(",")(1))).saveAsSequenceFile("/user/cloudera/scalaspark/departmentsSeq")

import org.apache.hadoop.mapreduce.lib.output._

val path="/user/cloudera/scalaspark/departmentsSeq"
dataRDD.map(x => (new Text(x.split(",")(0)), new Text(x.split(",")(1)))).saveAsNewAPIHadoopFile(path, classOf[Text], classOf[Text], classOf[SequenceFileOutputFormat[Text, Text]])

#reading sequence file
sc.sequenceFile("/user/cloudera/spark/departmentsSeq", classOf[IntWritable], classOf[Text]).map(rec => rec.toString()).collect().foreach(println)

import org.apache.spark.sql.hive.HiveContext
val sqlContext = new HiveContext(sc)
val depts = sqlContext.sql("select * from departments")
depts.collect().foreach(println)

sqlContext.sql("create table departmentsScalaSpark as select * from departments")
val depts = sqlContext.sql("select * from departmentsScalaSpark")
depts.collect().foreach(println)

#We can run hive INSERT, LOAD and any valid hive query in Hive context

#Make sure you copy departments.json to HDFS
#create departments.json on Linux file system
<record>
<department_id>2</department_id>
<department_name>Fitness</department_name>
</record>
{"department_id":2, "department_name":"Fitness"}
{"department_id":3, "department_name":"Footwear"}
{"department_id":4, "department_name":"Apparel"}
{"department_id":5, "department_name":"Golf"}
{"department_id":6, "department_name":"Outdoors"}
{"department_id":7, "department_name":"Fan Shop"}
{"department_id":8, "department_name":"TESTING"}
{"department_id":8000, "department_name":"TESTING"}

#copying to HDFS (using linux command line)
hadoop fs -put departments.json /user/cloudera/scalaspark

import org.apache.spark.sql.SQLContext
val sqlContext = new SQLContext(sc)
val departmentsJson = sqlContext.jsonFile("/user/cloudera/scalaspark/departments.json")
departmentsJson.registerTempTable("departmentsTable")
val departmentsData = sqlContext.sql("select * from departmentsTable")
departmentsData.collect().foreach(println)

#Writing data in json format
departmentsData.toJSON.saveAsTextFile("/user/cloudera/scalaspark/departmentsJson")

#Validating the data
hadoop fs -cat /user/cloudera/scalaspark/departmentsJson/part*

##############################################################################
# Developing word count program
# Create a file and type few lines and save it as wordcount.txt and copy to HDFS
# to /user/cloudera/wordcount.txt

val data = sc.textFile("/user/cloudera/wordcount.txt")
val dataFlatMap = data.flatMap(x => x.split(" "))
val dataMap = dataFlatMap.map(x => (x, 1))
val dataReduceByKey = dataMap.reduceByKey((x,y) => x + y)

dataReduceByKey.saveAsTextFile("/user/cloudera/wordcountoutput")

##############################################################################

# Join disparate datasets together using Spark
# Problem statement, get the revenue and number of orders from order_items on daily basis
val ordersRDD = sc.textFile("/user/cloudera/sqoop_import/orders")
val orderItemsRDD = sc.textFile("/user/cloudera/sqoop_import/order_items")

val ordersParsedRDD = ordersRDD.map(rec => (rec.split(",")(0).toInt, rec))
val orderItemsParsedRDD = orderItemsRDD.map(rec => (rec.split(",")(1).toInt, rec))
 
val ordersJoinOrderItems = orderItemsParsedRDD.join(ordersParsedRDD)
val revenuePerOrderPerDay = ordersJoinOrderItems.map(t => (t._2._2.split(",")(1), t._2._1.split(",")(4).toFloat))

# Get order count per day
val ordersPerDay = ordersJoinOrderItems.map(rec => rec._2._2.split(",")(1) + "," + rec._1).distinct()
val ordersPerDayParsedRDD = ordersPerDay.map(rec => (rec.split(",")(0), 1))
val totalOrdersPerDay = ordersPerDayParsedRDD.reduceByKey((x, y) => x + y)

# Get revenue per day from joined data
val totalRevenuePerDay = revenuePerOrderPerDay.reduceByKey(
  (total1, total2) => total1 + total2 
)

totalRevenuePerDay.sortByKey().collect().foreach(println)

# Joining order count per day and revenue per day
val finalJoinRDD = totalOrdersPerDay.join(totalRevenuePerDay)
finalJoinRDD.collect().foreach(println)

# Using Hive
import org.apache.spark.sql.hive.HiveContext
val sqlContext = new HiveContext(sc)
sqlContext.sql("set spark.sql.shuffle.partitions=10"); 

val joinAggData = sqlContext.sql("select o.order_date, round(sum(oi.order_item_subtotal), 2), count(distinct o.order_id) from orders o join order_items oi on o.order_id = oi.order_item_order_id group by o.order_date order by o.order_date")

joinAggData.collect().foreach(println)

# Using spark native sql
import org.apache.spark.sql.SQLContext, org.apache.spark.sql.Row
val sqlContext = new SQLContext(sc)
sqlContext.sql("set spark.sql.shuffle.partitions=10");

val ordersRDD = sc.textFile("/user/cloudera/sqoop_import/orders")
val ordersMap = ordersRDD.map(o => o.split(","))

case class Orders(order_id: Int, order_date: String, order_customer_id: Int, order_status: String)
val orders = ordersMap.map(o => Orders(o(0).toInt, o(1), o(2).toInt, o(3)))

import sqlContext.createSchemaRDD
orders.registerTempTable("orders")

val orderItemsRDD = sc.textFile("/user/cloudera/sqoop_import/order_items")
val orderItemsMap = orderItemsRDD.map(oi => oi.split(","))

case class OrderItems
  (order_item_id: Int,
   order_item_order_id: Int,
   order_item_product_id: Int,
   order_item_quantity: Int,
   order_item_subtotal: Float,
   order_item_product_price: Float
  )

val orderItems = sc.textFile("/user/cloudera/sqoop_import/order_items").
  map(rec => rec.split(",")).
  map(oi => OrderItems(oi(0).toInt, oi(1).toInt, oi(2).toInt, oi(3).toInt, oi(4).toFloat, oi(5).toFloat))

orderItems.registerTempTable("order_items")

val joinAggData = sqlContext.sql("select o.order_date, sum(oi.order_item_subtotal), " +
  "count(distinct o.order_id) from orders o join order_items oi " +
  "on o.order_id = oi.order_item_order_id " +
  "group by o.order_date order by o.order_date")

joinAggData.collect().foreach(println)

##############################################################################

# Calculate aggregate statistics (e.g., average or sum) using Spark
#sum
val ordersRDD = sc.textFile("/user/cloudera/sqoop_import/orders")
ordersRDD.count()

val orderItemsRDD = sc.textFile("/user/cloudera/sqoop_import/order_items")
val orderItemsMap = orderItemsRDD.map(rec => (rec.split(",")(4).toDouble))
orderItemsMap.take(5).foreach(println)

val orderItemsReduce = orderItemsMap.reduce((acc, value) => acc + value)

#Get max priced product from products table
#There is one record which is messing up default , delimiters
#Clean it up (we will see how we can filter with out deleting the record later)
hadoop fs -get /user/cloudera/sqoop_import/products
#Delete the record with product_id 685
hadoop fs -put -f products/part* /user/cloudera/sqoop_import/products

#pyspark script to get the max priced product
val productsRDD = sc.textFile("/user/cloudera/sqoop_import/products")
val productsMap = productsRDD.map(rec => rec)
productsMap.reduce((rec1, rec2) => (
  if(rec1.split(",")(4).toFloat >= rec2.split(",")(4).toFloat)
    rec1
  else 
    rec2)
)

#avg
val revenue = sc.textFile("/user/cloudera/sqoop_import/order_items").
  map(rec => rec.split(",")(4).toDouble).
  reduce((rev1, rev2) => rev1 + rev2)
val totalOrders = sc.textFile("/user/cloudera/sqoop_import/order_items").
  map(rec => rec.split(",")(1).toInt).
  distinct().
  count()

#Number of orders by status
val ordersRDD = sc.textFile("/user/cloudera/sqoop_import/orders")
val ordersMap = ordersRDD.map(rec => (rec.split(",")(3), 1))
ordersMap.countByKey().foreach(println)

#groupByKey is not very efficient for aggregations. It does not use combiner
val ordersByStatus = ordersMap.groupByKey().map(t => (t._1, t._2.sum))

#reduceByKey uses combiner - both reducer logic and combiner logic are same
val ordersByStatus = ordersMap.reduceByKey((acc, value) => acc + value)

#combineByKey can be used when reduce logic and combine logic are different
#Both reduceByKey and combineByKey expects type of input data and output data are same
val ordersByStatus = ordersMap.combineByKey(value => 1, (acc: Int, value: Int) => acc+value,  (acc: Int, value: Int) => acc+value)

#aggregateByKey can be used when reduce logic and combine logic is different
#Also type of input data and output data need not be same
val ordersMap = ordersRDD.map(rec =>  (rec.split(",")(3), rec))
val ordersByStatus = ordersMap.aggregateByKey(0, (acc, value) => acc+1, (acc, value) => acc+value)
ordersByStatus.collect().foreach(println)

#Number of orders by order date and order status
#Key orderDate and orderStatus
val ordersRDD = sc.textFile("/user/cloudera/sqoop_import/orders")
val ordersMapRDD = ordersRDD.map(rec => ((rec.split(",")(1), rec.split(",")(3)), 1))
val ordersByStatusPerDay = ordersMapRDD.reduceByKey((v1, v2) => v1+v2)

ordersByStatusPerDay.collect().foreach(println)

#Total Revenue per day
val ordersRDD = sc.textFile("/user/cloudera/sqoop_import/orders")
val orderItemsRDD = sc.textFile("/user/cloudera/sqoop_import/order_items")

val ordersParsedRDD = ordersRDD.map(rec => (rec.split(",")(0), rec))
val orderItemsParsedRDD = orderItemsRDD.map(rec => (rec.split(",")(1), rec))

val ordersJoinOrderItems = orderItemsParsedRDD.join(ordersParsedRDD)
val ordersJoinOrderItemsMap = ordersJoinOrderItems.map(t => (t._2._2.split(",")(1), t._2._1.split(",")(4).toFloat))

val revenuePerDay = ordersJoinOrderItemsMap.reduceByKey((acc, value) => acc + value)
revenuePerDay.collect().foreach(println)

#average
#average revenue per day
#Parse Orders (key order_id)
#Parse Order items (key order_item_order_id)
#Join the data sets
#Parse joined data and get (order_date, order_id) as key  and order_item_subtotal as value
#Use appropriate aggregate function to get sum(order_item_subtotal) for each order_date, order_id combination
#Parse data to discard order_id and get order_date as key and sum(order_item_subtotal) per order as value
#Use appropriate aggregate function to get sum(order_item_subtotal) per day and count(distinct order_id) per day
#Parse data and apply average logic
val ordersRDD = sc.textFile("/user/cloudera/sqoop_import/orders")
val orderItemsRDD = sc.textFile("/user/cloudera/sqoop_import/order_items")

val ordersParsedRDD = ordersRDD.map(rec => (rec.split(",")(0), rec))
val orderItemsParsedRDD = orderItemsRDD.map(rec => (rec.split(",")(1), rec))

val ordersJoinOrderItems = orderItemsParsedRDD.join(ordersParsedRDD)
val ordersJoinOrderItemsMap = ordersJoinOrderItems.map(t => ((t._2._2.split(",")(1), t._1), t._2._1.split(",")(4).toFloat))

val revenuePerDayPerOrder = ordersJoinOrderItemsMap.reduceByKey((acc, value) => acc + value)
val revenuePerDayPerOrderMap = revenuePerDayPerOrder.map(rec => (rec._1._1, rec._2))

val revenuePerDay = revenuePerDayPerOrderMap.aggregateByKey((0.0, 0))(
(acc, revenue) => (acc._1 + revenue, acc._2 + 1), 
(total1, total2) => (total1._1 + total2._1, total1._2 + total2._2) 
)

revenuePerDay.collect().foreach(println)

val avgRevenuePerDay = revenuePerDay.map(x => (x._1, x._2._1/x._2._2))

#Customer id with max revenue
val ordersRDD = sc.textFile("/user/cloudera/sqoop_import/orders")
val orderItemsRDD = sc.textFile("/user/cloudera/sqoop_import/order_items")

val ordersParsedRDD = ordersRDD.map(rec => (rec.split(",")(0), rec))
val orderItemsParsedRDD = orderItemsRDD.map(rec => (rec.split(",")(1), rec))

val ordersJoinOrderItems = orderItemsParsedRDD.join(ordersParsedRDD)
val ordersPerDayPerCustomer = ordersJoinOrderItems.map(rec => ((rec._2._2.split(",")(1), rec._2._2.split(",")(2)), rec._2._1.split(",")(4).toFloat))
val revenuePerDayPerCustomer = ordersPerDayPerCustomer.reduceByKey((x, y) => x + y)

val revenuePerDayPerCustomerMap = revenuePerDayPerCustomer.map(rec => (rec._1._1, (rec._1._2, rec._2)))
val topCustomerPerDaybyRevenue = revenuePerDayPerCustomerMap.reduceByKey((x, y) => (if(x._2 >= y._2) x else y))

#Using regular function
def findMax(x: (String, Float), y: (String, Float)): (String, Float) = {
  if(x._2 >= y._2)
    return x
  else
    return y
}

val topCustomerPerDaybyRevenue = revenuePerDayPerCustomerMap.reduceByKey((x, y) => findMax(x, y))

# Using Hive Context
import org.apache.spark.sql.hive.HiveContext
val hiveContext = new HiveContext(sc)
hiveContext.sql("set spark.sql.shuffle.partitions=10");

hiveContext.sql("select o.order_date, sum(oi.order_item_subtotal)/count(distinct oi.order_item_order_id) from orders o join order_items oi on o.order_id = oi.order_item_order_id group by o.order_date order by o.order_date").collect().foreach(println)

# This query works in hive
select * from (select q.order_date, q.order_customer_id, q.order_item_subtotal, 
max(q.order_item_subtotal) over (partition by q.order_date) max_order_item_subtotal 
from (select o.order_date, o.order_customer_id, sum(oi.order_item_subtotal) order_item_subtotal 
from orders o join order_items oi 
on o.order_id = oi.order_item_order_id 
group by o.order_date, o.order_customer_id) q) s
where s.order_item_subtotal = s.max_order_item_subtotal
order by s.order_date;

select * from (
select o.order_date, o.order_customer_id, sum(oi.order_item_subtotal) order_item_subtotal 
from orders o join order_items oi 
on o.order_id = oi.order_item_order_id 
group by o.order_date, o.order_customer_id) q1
join
(select q.order_date, max(q.order_item_subtotal) order_item_subtotal
from (select o.order_date, o.order_customer_id, sum(oi.order_item_subtotal) order_item_subtotal
from orders o join order_items oi
on o.order_id = oi.order_item_order_id
group by o.order_date, o.order_customer_id) q
group by q.order_date) q2
on q1.order_date = q2.order_date and q1.order_item_subtotal = q2.order_item_subtotal
order by q1.order_date;

##########################################################################################

# Using data frames (only works with spark 1.3.0 or later)
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._
sqlContext.sql("set spark.sql.shuffle.partitions=10");

case class Orders(orderId: Int, orderDate: String, orderCustomerId: Int, orderStatus: String)

val orders = sc.textFile("/user/cloudera/sqoop_import/orders").
  map(rec => rec.split(",")).
  map(o => Orders(o(0).toInt, o(1), o(2).toInt, o(3))).toDF()

orders.registerTempTable("orders")
val ordersData = sqlContext.sql("select * from orders")
ordersData.collect().take(10).foreach(println)

case class OrderItems
  (orderItemId: Int, 
   orderItemOrderId: Int, 
   orderItemProductId: Int, 
   orderItemQuantity: Int, 
   orderItemSubtotal: Float, 
   orderItemProductPrice: Float
  )

val orderItems = sc.textFile("/user/cloudera/sqoop_import/order_items").
  map(rec => rec.split(",")).
  map(oi => OrderItems(oi(0).toInt, oi(1).toInt, oi(2).toInt, oi(3).toInt, oi(4).toFloat, oi(5).toFloat)).
  toDF()

orderItems.registerTempTable("orderItems")
val orderItemsData = sqlContext.sql("select * from orderItems")
orderItemsData.collect().take(10).foreach(println)

val ordersJoinOrderItems = sqlContext.sql("select o.orderDate, sum(oi.orderItemSubtotal), count(o.orderId) from orders o join orderItems oi on o.orderId = oi.orderItemOrderId group by o.orderDate")
val ordersJoinOrderItems = sqlContext.sql("select o.orderDate, sum(oi.orderItemSubtotal), count(distinct o.orderId), sum(oi.orderItemSubtotal)/count(distinct o.orderId) from orders o join orderItems oi on o.orderId = oi.orderItemOrderId group by o.orderDate order by o.orderDate")
ordersJoinOrderItems.collect().take(10).foreach(println)

##########################################################

# Filter data into a smaller dataset using Spark
val ordersRDD = sc.textFile("/user/cloudera/sqoop_import/orders")
ordersRDD.filter(line => line.split(",")(3).equals("COMPLETE")).take(5).foreach(println)
ordersRDD.filter(line => line.split(",")(3).contains("PENDING")).take(5).foreach(println)
ordersRDD.filter(line => line.split(",")(0).toInt > 100).take(5).foreach(println)
ordersRDD.filter(line => line.split(",")(0).toInt > 100 || line.split(",")(3).contains("PENDING")).take(5).foreach(println)
ordersRDD.filter(line => line.split(",")(0).toInt > 1000 && 
    (line.split(",")(3).contains("PENDING") || line.split(",")(3).equals("CANCELLED"))).
    take(5).
    foreach(println)
ordersRDD.filter(line => line.split(",")(0).toInt > 1000 && 
    !line.split(",")(3).equals("COMPLETE")).
    take(5).
    foreach(println)

#Check if there are any cancelled orders with amount greater than 1000$
#Get only cancelled orders
#Join orders and order items
#Generate sum(order_item_subtotal) per order
#Filter data which amount to greater than 1000$

val ordersRDD = sc.textFile("/user/cloudera/sqoop_import/orders")
val orderItemsRDD = sc.textFile("/user/cloudera/sqoop_import/order_items")

val ordersParsedRDD = ordersRDD.filter(rec => rec.split(",")(3).contains("CANCELED")).
  map(rec => (rec.split(",")(0).toInt, rec))
val orderItemsParsedRDD = orderItemsRDD.
  map(rec => (rec.split(",")(1).toInt, rec.split(",")(4).toFloat))
val orderItemsAgg = orderItemsParsedRDD.reduceByKey((acc, value) => (acc + value))

val ordersJoinOrderItems = orderItemsAgg.join(ordersParsedRDD)

ordersJoinOrderItems.filter(rec => rec._2._1 >= 1000).take(5).foreach(println)

#Using SQL
import org.apache.spark.sql.hive.HiveContext
val sqlContext = new HiveContext(sc)

sqlContext.sql("select * from (select o.order_id, sum(oi.order_item_subtotal) as order_item_revenue from orders o join order_items oi on o.order_id = oi.order_item_order_id where o.order_status = 'CANCELED' group by o.order_id) q where order_item_revenue >= 1000").count()

##########################################################

# Write a query that produces ranked or sorted data using Spark

#Global sorting and ranking
val orders = sc.textFile("/user/cloudera/sqoop_import/orders")
orders.map(rec => (rec.split(",")(0).toInt, rec)).sortByKey().collect().foreach(println)
orders.map(rec => (rec.split(",")(0).toInt, rec)).sortByKey(false).take(5).foreach(println)
orders.map(rec => (rec.split(",")(0).toInt, rec)).top(5).foreach(println)

orders.map(rec => (rec.split(",")(0).toInt, rec)).
  takeOrdered(5).
  foreach(println)

orders.map(rec => (rec.split(",")(0).toInt, rec)).
  takeOrdered(5)(Ordering[Int].reverse.on(x => x._1)).
  foreach(println)

orders.takeOrdered(5)(Ordering[Int].on(x => x.split(",")(0).toInt)).foreach(println)
orders.takeOrdered(5)(Ordering[Int].reverse.on(x => x.split(",")(0).toInt)).foreach(println)


val products = sc.textFile("/user/cloudera/sqoop_import/products")
val productsMap = products.map(rec => (rec.split(",")(1), rec))
val productsGroupBy = productsMap.groupByKey()
productsGroupBy.collect().foreach(println)

#Get data sorted by product price per category
#You can use map or flatMap, if you want to see one record per line you need to use flatMap
#Map will return the list
productsGroupBy.map(rec => (rec._2.toList.sortBy(k => k.split(",")(4).toFloat))).
  take(100).
  foreach(println)

productsGroupBy.map(rec => (rec._2.toList.sortBy(k => -k.split(",")(4).toFloat))).
  take(100).
  foreach(println)

productsGroupBy.flatMap(rec => (rec._2.toList.sortBy(k => -k.split(",")(4).toFloat))).
  take(100).
  foreach(println)

def getAll(rec: (String, Iterable[String])): Iterable[String] = {
  return rec._2
}
productsGroupBy.flatMap(x => getAll(x)).collect().foreach(println)

#To get topN products by price in each category
def getTopN(rec: (String, Iterable[String]), topN: Int): Iterable[String] = {
  val x: List[String] = rec._2.toList.sortBy(k => -k.split(",")(4).toFloat).take(topN)
  return x
}

val products = sc.textFile("/user/cloudera/sqoop_import/products")
val productsMap = products.map(rec => (rec.split(",")(1), rec))
productsMap.groupByKey().flatMap(x => getTopN(x, 2)).collect().foreach(println)

#To get topN priced products by category
def getTopDenseN(rec: (String, Iterable[String]), topN: Int): Iterable[String] = {
  var prodPrices: List[Float] = List()
  var topNPrices: List[Float] = List()
  var sortedRecs: List[String] = List()
  for(i <- rec._2) {
    prodPrices = prodPrices:+ i.split(",")(4).toFloat
  }
  topNPrices = prodPrices.distinct.sortBy(k => -k).take(topN)
  sortedRecs = rec._2.toList.sortBy(k => -k.split(",")(4).toFloat) 
  var x: List[String] = List()
  for(i <- sortedRecs) {
    if(topNPrices.contains(i.split(",")(4).toFloat))
      x = x:+ i 
  }
  return x
}

productsMap.groupByKey().flatMap(x => getTopDenseN(x, 2)).collect().foreach(println)

#Sorting using queries
#Global sorting and ranking
select * from products order by product_price desc;
select * from products order by product_price desc limit 10;

#By key sorting
#Using order by is not efficient, it serializes
select * from products order by product_category_id, product_price desc;

#Using distribute by sort by (to distribute sorting and scale it up)
select * from products distribute by product_category_id sort by product_price desc;

#By key ranking (in Hive we can use windowing/analytic functions)
select * from (select p.*, 
dense_rank() over (partition by product_category_id order by product_price desc) dr
from products p
distribute by product_category_id) q
where dr <= 2 order by product_category_id, dr;
