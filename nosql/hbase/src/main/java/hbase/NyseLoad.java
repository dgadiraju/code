package hbase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

public class NyseLoad {

	static Configuration conf = HBaseConfiguration.create();
	static NyseParser nyseParser = new NyseParser();
	static Table table;

	static final Logger logger = Logger.getLogger(NyseLoad.class);
	
	public static Put buildPutList(Table table, NyseParser nyseRecord)
			throws RetriesExhaustedWithDetailsException, InterruptedIOException, IOException {

		SimpleDateFormat formatter = new SimpleDateFormat("dd-MMM-yyyy");
		String transactionDate = null;
		try {
			transactionDate = (new SimpleDateFormat("yyyy-MM-dd")
					.format(formatter.parse(nyseRecord.getTransactionDate())))
					.toString();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if(transactionDate == null || transactionDate.equals("null")) 
			System.out.println(nyseRecord.getTransactionDate());
		
		Put put = new Put(Bytes.toBytes(nyseRecord.getStockTicker() + ","
				+ transactionDate)); // Key

		put.addColumn(Bytes.toBytes("sd"), Bytes.toBytes("op"),
				Bytes.toBytes(nyseRecord.getOpenPrice().floatValue()));
		put.addColumn(Bytes.toBytes("sd"), Bytes.toBytes("hp"),
				Bytes.toBytes(nyseRecord.getHighPrice().floatValue()));
		put.addColumn(Bytes.toBytes("sd"), Bytes.toBytes("lp"),
				Bytes.toBytes(nyseRecord.getLowPrice().floatValue()));
		put.addColumn(Bytes.toBytes("sd"), Bytes.toBytes("cp"),
				Bytes.toBytes(nyseRecord.getClosePrice().floatValue()));
		put.addColumn(Bytes.toBytes("sd"), Bytes.toBytes("v"),
				Bytes.toBytes(nyseRecord.getVolume().intValue()));

		return put;

	}
	
	public static void loadPutList(List<Put> puts, Table table) throws IOException {
		table.put(puts);
	}

	public static void readFilesAndLoad(Table table, String nysePath) {
		int counter = 1;
		
		List<Put> puts = new ArrayList<Put>();
		
		File localInputFolder = new File(nysePath);
		File[] listOfDirectories = localInputFolder.listFiles();
		for (File dir : listOfDirectories) {
			if (dir.isDirectory()) {
				File[] files = dir.listFiles();
				for (File file : files) {
					BufferedReader br = null;
					if (file.getName().endsWith("csv")) {
						try {

							String sCurrentLine;

							br = new BufferedReader(new FileReader(file));

							while ((sCurrentLine = br.readLine()) != null) {
								if(++counter%10000 == 0)
									logger.info(counter);
								nyseParser.parse(sCurrentLine);
								puts.add(buildPutList(table, nyseParser));
							}
							loadPutList(puts, table);

						} catch (IOException e) {
							e.printStackTrace();
						} finally {
							try {
								if (br != null)
									br.close();
							} catch (IOException ex) {
								ex.printStackTrace();
							}
						}
					}

				}

			}

		}
	}

	public static void main(String[] args) throws IOException {

		conf.set("hbase.zookeeper.quorum", args[0]);
		conf.set("hbase.zookeeper.property.clientPort", "2181");

		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("nyse:stock_data"));

		readFilesAndLoad(table, args[1]);

		table.close();
		connection.close();

	}
}
