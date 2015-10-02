package nyse.partitioners;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import nyse.keyvalues.LongPairPrimitive;

public class FirstKeyLongPairPartitioner extends Partitioner<LongPairPrimitive, Text>{

	@Override
	public int getPartition(LongPairPrimitive key, Text value, int numPartitions) {
		long partValue = new Long(new Long(key.getFirst()).toString().substring(0, 6)).longValue();

		return (int) partValue % numPartitions;
	}

}
