package demo.cards.reducers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NoKeyRecordCountReducer extends
		Reducer<Text, IntWritable, NullWritable, IntWritable> {

	@Override
	public void reduce(Text key, Iterable<IntWritable> records, Context context)
			throws IOException, InterruptedException {
		int sum = 0;

		for (IntWritable record : records) {
			sum += record.get();
		}

		context.write(NullWritable.get(), new IntWritable(sum));
		//output for our largedeck
		//count	54525952
		
		//output for out deckofcards
		//count	52
		
	}
}
