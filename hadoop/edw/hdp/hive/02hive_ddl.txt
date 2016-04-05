-- Create sample database
-- CREATE DATABASE IF NOT EXISTS cards;

CREATE TABLE deck_of_cards (
COLOR string,
SUIT string,
PIP string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

--Download deckofcards.txt from github repository www.github.com/dgadiraju/data
--mkdir -p ~/demo/data/cards
--copy file deckofcards.txt to ~/demo/data/cards
-- Load data from local file system into hive table (append to existing table)
LOAD DATA LOCAL INPATH '/root/demo/data/cards/deckofcards.txt' INTO TABLE deck_of_cards;

--Load data from HDFS into hive table (append data to existing table), file user /user/root/cards will be deleted
LOAD DATA INPATH '/user/root/cards/deckofcards.txt' INTO TABLE deck_of_cards;

-- Loads data from local file system (overwrite existing data)
LOAD DATA LOCAL INPATH '/root/demo/data/cards/deckofcards.txt' OVERWRITE INTO TABLE deck_of_cards;

CREATE EXTERNAL TABLE deck_of_cards_external (
COLOR string,
SUIT string,
PIP string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/apps/hive/warehouse/cards.db/deck_of_cards';

-- Create ods and edw database for retail_db@mysql
-- CREATE DATABASE IF NOT EXISTS retail_ods;
-- CREATE DATABASE retail_edw;
-- CREATE DATABASE retail_stage;

-- HDPCD - Define a Hive-managed table 
USE retail_stage;
CREATE TABLE orders_demo (
order_id int,
order_date string,
order_customer_id int,
order_status string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- HDPCD - Define a Hive external table
-- Download the data from github, unzip and copy deckofcards.txt 
-- Run on terminal on your PC/Mac to copy data to sandbox
-- scp ./Documents/Training/GoogleDrive/Training/data/cards/deckofcards.txt root@sandbox.hortonworks.com:~
-- On sandbox
hadoop fs -mkdir /user/root/cards
hadoop fs -put deckofcards.txt /user/root/cards
hadoop fs -ls /user/root/cards
-- launch hive by running "hive"
CREATE DATABASE IF NOT EXISTS cards;
USE cards;
CREATE EXTERNAL TABLE deck_of_cards_external
(color string,
suit string,
pip string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/user/root/cards';


-- Create ods tables (mostly they will follow same structure, except additional audit columns)
use retail_ods;
CREATE TABLE categories (
category_id int,
category_department_id int,
category_name string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE TABLE customers (
customer_id       int,
customer_fname    string,
customer_lname    string,
customer_email    string,
customer_password string,
customer_street   string,
customer_city     string,
customer_state    string,
customer_zipcode  string 
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE TABLE departments (
department_id int,
department_name string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE TABLE orders (
order_id int,
order_date string,
order_customer_id int,
order_status string
)
PARTITIONED BY (order_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE TABLE order_items (
order_item_id int,
order_item_order_id int,
order_item_order_date string,
order_item_product_id int,
order_item_quantity smallint,
order_item_subtotal float,
order_item_product_price float
)
PARTITIONED BY (order_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE TABLE orders_bucket (
order_id int,
order_date string,
order_customer_id int,
order_status string
)
CLUSTERED BY (order_id) INTO 16 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE TABLE order_items_bucket (
order_item_id int,
order_item_order_id int,
order_item_order_date string,
order_item_product_id int,
order_item_quantity smallint,
order_item_subtotal float,
order_item_product_price float
)
CLUSTERED BY (order_item_order_id) INTO 16 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE TABLE products (
product_id int, 
product_category_id int,
product_name string,
product_description string,
product_price float,
product_image string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

-- Create edw tables (following dimension model)
use retail_edw;
CREATE TABLE products_dimension (
product_id int,
product_name string,
product_description string,
product_price float,
product_category_name string,
product_department_name string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE TABLE order_fact (
order_item_order_id int,
order_item_order_date string,
order_item_product_id int,
order_item_quantity smallint,
order_item_subtotal float,
order_item_product_price float
)
PARTITIONED BY (product_category_department string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

-- Create external tables for retail_stage
use retail_stage;

CREATE EXTERNAL TABLE categories
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION 'hdfs:///apps/hive/warehouse/retail_stage.db/categories'
TBLPROPERTIES ('avro.schema.url'='hdfs://sandbox.hortonworks.com/user/root/retail_stage/sqoop_import_categories.avsc');

CREATE EXTERNAL TABLE customers
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION 'hdfs:///apps/hive/warehouse/retail_stage.db/customers'
TBLPROPERTIES ('avro.schema.url'='hdfs://sandbox.hortonworks.com/user/root/retail_stage/customers.avsc');

CREATE EXTERNAL TABLE departments
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION 'hdfs:///apps/hive/warehouse/retail_stage.db/departments'
TBLPROPERTIES ('avro.schema.url'='hdfs://sandbox.hortonworks.com/user/root/retail_stage/departments.avsc');

CREATE EXTERNAL TABLE orders
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION 'hdfs:///apps/hive/warehouse/retail_stage.db/orders'
TBLPROPERTIES ('avro.schema.url'='hdfs://sandbox.hortonworks.com/user/root/retail_stage/orders.avsc');

CREATE EXTERNAL TABLE order_items
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION 'hdfs:///apps/hive/warehouse/retail_stage.db/order_items'
TBLPROPERTIES ('avro.schema.url'='hdfs://sandbox.hortonworks.com/user/root/retail_stage/order_items.avsc');

CREATE EXTERNAL TABLE products
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION 'hdfs:///apps/hive/warehouse/retail_stage.db/products'
TBLPROPERTIES ('avro.schema.url'='hdfs://sandbox.hortonworks.com/user/root/retail_stage/products.avsc');

CREATE TABLE departments_delta (
department_id int,
department_name string,
update_date string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;
