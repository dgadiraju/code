package demo.cards.mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RecordMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
/*	BLACK|SPADE|2
	BLACK|SPADE|3
	BLACK|SPADE|4
	BLACK|SPADE|5
	BLACK|SPADE|6
	BLACK|SPADE|7
	BLACK|SPADE|8
	BLACK|SPADE|9
	BLACK|SPADE|10
	BLACK|SPADE|J
	BLACK|SPADE|Q
	BLACK|SPADE|K
	BLACK|SPADE|A
	BLACK|CLUB|2
	BLACK|CLUB|3
	BLACK|CLUB|4
	BLACK|CLUB|5
	BLACK|CLUB|6
	BLACK|CLUB|7
	BLACK|CLUB|8
	BLACK|CLUB|9
	BLACK|CLUB|10
	BLACK|CLUB|J
	BLACK|CLUB|Q
	BLACK|CLUB|K
	BLACK|CLUB|A
	RED|DIAMOND|2
	RED|DIAMOND|3
	RED|DIAMOND|4
	RED|DIAMOND|5
	RED|DIAMOND|6
	RED|DIAMOND|7
	RED|DIAMOND|8
	RED|DIAMOND|9
	RED|DIAMOND|10
	RED|DIAMOND|J
	RED|DIAMOND|Q
	RED|DIAMOND|K
	RED|DIAMOND|A
	RED|HEART|2
	RED|HEART|3
	RED|HEART|4
	RED|HEART|5
	RED|HEART|6
	RED|HEART|7
	RED|HEART|8
	RED|HEART|9
	RED|HEART|10
	RED|HEART|J
	RED|HEART|Q
	RED|HEART|K
	RED|HEART|A */
	public void map(LongWritable key, Text record, Context context)
			throws IOException, InterruptedException {
		context.write(new Text("count"), new IntWritable(1));
	}
	/*<count, 1>
	 *<count, 1>
	 *<count, 1>
	 * there will be 52
	 */

}