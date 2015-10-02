package nyse.avgstockvolpermonth;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import nyse.keyvalues.LongPair;
import nyse.keyvalues.TextPair;
import nyse.parsers.NYSEParser;

public class AvgStockVolPerMonthMapper extends Mapper<LongWritable, Text, TextPair, LongPair> {

	private static NYSEParser parser = new NYSEParser();
	private static TextPair mapOutputKey = new TextPair();
	private static LongPair mapOutputValue = new LongPair();

	private static Set<String> stockTickers = new HashSet<String>();

	protected void setup(Context context) throws IOException, InterruptedException {
		String stockTicker = context.getConfiguration().get("filter.by.stock");
		if (stockTicker != null) {
			String[] tickers = stockTicker.split(",");

			for (String ticker : tickers) {
				stockTickers.add(ticker);
			}
		}
	}

	public void map(LongWritable lineOffset, Text record, Context context) throws IOException, InterruptedException {
		parser.parse(record.toString());

		if (stockTickers.isEmpty() || (!stockTickers.isEmpty() && stockTickers.contains(parser.getStockTicker()))) {
			mapOutputKey.setFirst(new Text(parser.getTradeMonth()));
			mapOutputKey.setSecond(new Text(parser.getStockTicker()));

			mapOutputValue.setFirst(new LongWritable(parser.getVolume()));
			mapOutputValue.setSecond(new LongWritable(1));

			context.write(mapOutputKey, mapOutputValue);
		}
	}

}
