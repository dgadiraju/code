package nyse.avgstockvolpermonth;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import nyse.keyvalues.LongPair;
import nyse.keyvalues.TextPair;

public class AvgStockVolPerMonthReducer extends Reducer<TextPair, LongPair, TextPair, LongPair> {
	
	private static Long totalVolume = new Long(0);
	private static Long noOfRecords = new Long(0);
	private static Long avgVolume = new Long(0);
	private static LongPair result = new LongPair();
	
	public void reduce(TextPair key, Iterable<LongPair> values, Context context) throws IOException, InterruptedException {
		totalVolume = new Long(0);
		noOfRecords = new Long(0);

		for(LongPair value : values) {
			totalVolume += value.getFirst().get();
			noOfRecords += value.getSecond().get();
		}
		
		avgVolume = totalVolume / noOfRecords;
		result.setFirst(new LongWritable(avgVolume));
		result.setSecond(new LongWritable(noOfRecords));
		context.write(key, result);
	}

}
