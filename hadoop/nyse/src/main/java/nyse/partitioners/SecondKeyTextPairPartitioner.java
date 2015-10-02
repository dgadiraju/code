package nyse.partitioners;

import org.apache.hadoop.mapreduce.Partitioner;

import nyse.keyvalues.LongPair;
import nyse.keyvalues.TextPair;

public class SecondKeyTextPairPartitioner extends Partitioner<TextPair, LongPair>{

	@Override
	public int getPartition(TextPair key, LongPair value, int numPartitions) {
		int partitionValue = 0;
		partitionValue = (key.getSecond().hashCode() & Integer.MAX_VALUE) % numPartitions;
		return partitionValue;
	}

}
