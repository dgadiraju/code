package nyse.topthreestocksbyvolume;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import nyse.keyvalues.LongPairPrimitive;

public class TopThreeStocksByVolumePerDayReducer extends Reducer<LongPairPrimitive, Text, NullWritable, Text> {
	public enum NOTRADEDAYS {
		NOTRADE
	}
	
	private MultipleOutputs<NullWritable, Text> multipleOutputs;

	protected void setup(Context context) throws IOException, InterruptedException {
		multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
	}

	public void reduce(LongPairPrimitive inputKey, Iterable<Text> records, Context context)
			throws IOException, InterruptedException {
		int cnt = 0;
		for (Text record : records) {
			if (inputKey.getSecond() == 0) {
				context.getCounter(NOTRADEDAYS.NOTRADE).increment(1);
				break;
			}
			if (cnt <= 2) {
				String basePath = String.format("trademonth=%s/part",
						new Long(inputKey.getFirst()).toString().substring(0, 6));
				multipleOutputs.write(NullWritable.get(), record, basePath);
			} else
				break;
			cnt++;
		}
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		multipleOutputs.close();
	}
}
