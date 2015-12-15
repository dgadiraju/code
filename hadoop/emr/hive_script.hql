dfs -cp s3://nyseitversity/input/raw_data /user/hadoop/raw_data;

create external table nyse_2009_s3
(stockticker string,
tradedate string,
openprice float,
highprice float,
lowprice float,
closeprice float,
volume bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://nyseitversity/input/raw_data/NYSE_2009';

create external table nyse_2009_hdfs
(stockticker string,
tradedate string,
openprice float,
highprice float,
lowprice float,
closeprice float,
volume bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/hadoop/raw_data/NYSE_2009';
