package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class GettingStarted {

	static Configuration conf = HBaseConfiguration.create();

	public static void main(String[] args) throws Exception {
		conf.set("hbase.zookeeper.quorum", "hadoop271.itversity.com");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		
		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("demo"));
		
		Scan scan1 = new Scan();
		ResultScanner scanner1 = table.getScanner(scan1);

		for (Result res : scanner1) {
			System.out.println(Bytes.toString(res.getRow()));
			System.out.println(Bytes.toString(res.getValue("cf1".getBytes(), "column1".getBytes())));
			System.out.println(Bytes.toString(res.getValue("cf1".getBytes(), "column2".getBytes())));
		}

		scanner1.close();

		Put put = new Put("3".getBytes());

		put.addColumn("cf1".getBytes(), "column1".getBytes(), "value1".getBytes());
		put.addColumn("cf1".getBytes(), "column2".getBytes(), "value2".getBytes());

		table.put(put);

		Get get = new Get("3".getBytes());
		Result getResult = table.get(get);
		System.out.println("Printing colunns for rowkey 3");
		System.out.println(Bytes.toString(getResult.getValue("cf1".getBytes(), "column1".getBytes())));
		System.out.println(Bytes.toString(getResult.getValue("cf1".getBytes(), "column2".getBytes())));

		scanner1 = table.getScanner(scan1);
		System.out.println("Before Delete");
		for (Result res : scanner1) {
			System.out.println(Bytes.toString(res.getRow()));
			System.out.println(Bytes.toString(res.getValue("cf1".getBytes(), "column1".getBytes())));
			System.out.println(Bytes.toString(res.getValue("cf1".getBytes(), "column2".getBytes())));
		}

		scanner1.close();

		Delete del = new Delete("3".getBytes());
		table.delete(del);
		
		System.out.println("After Delete");

		scanner1 = table.getScanner(scan1);
		
		for (Result res : scanner1) {
			System.out.println(Bytes.toString(res.getRow()));
			System.out.println(Bytes.toString(res.getValue("cf1".getBytes(), "column1".getBytes())));
			System.out.println(Bytes.toString(res.getValue("cf1".getBytes(), "column2".getBytes())));
		}

		scanner1.close();
		table.close();
		connection.close();

	}

}
