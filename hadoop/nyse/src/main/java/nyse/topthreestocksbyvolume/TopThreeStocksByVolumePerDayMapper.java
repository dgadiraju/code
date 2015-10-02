package nyse.topthreestocksbyvolume;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import nyse.keyvalues.LongPairPrimitive;
import nyse.parsers.NYSEParser;

public class TopThreeStocksByVolumePerDayMapper extends Mapper<LongWritable, Text, LongPairPrimitive, Text> {
	NYSEParser parser = new NYSEParser();
	private static LongPairPrimitive mapOutputKey = new LongPairPrimitive();

	public void map(LongWritable lineOffset, Text record, Context context) throws IOException, InterruptedException {
		parser.parse(record.toString());

		mapOutputKey.setFirst(parser.getTradeDateNumeric());
		mapOutputKey.setSecond(parser.getVolume());

		context.write(mapOutputKey, record);

	}

}
