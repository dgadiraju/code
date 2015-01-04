package demo.cards.drivers;

/**
 * Created with IntelliJ IDEA.
 * User: cloudera
 * Date: 9/22/13
 * Time: 1:27 PM
 * To change this template use File | Settings | File Templates.
 */

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import demo.cards.parsers.CardParser;

import java.io.IOException;

public class CardCountByPip extends Configured implements Tool {

	public static class PipPartitioner extends Partitioner<Text, IntWritable> {
		@Override
		public int getPartition(Text key, IntWritable value, int numPartitions) {
			return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	// Use this partitioner with mapreduce.job.reduces=13
	public static class PipPartitionerNoSkew extends
			Partitioner<Text, IntWritable> {
		@Override
		public int getPartition(Text key, IntWritable value, int numPartitions) {
			String str = key.toString();
			if (numPartitions == 13) {
				if (str.equals("2"))
					return 0;
				else if (str.equals("3"))
					return 1;
				else if (str.equals("4"))
					return 2;
				else if (str.equals("5"))
					return 3;
				else if (str.equals("6"))
					return 4;
				else if (str.equals("7"))
					return 5;
				else if (str.equals("8"))
					return 6;
				else if (str.equals("9"))
					return 7;
				else if (str.equals("10"))
					return 8;
				else if (str.equals("J"))
					return 9;
				else if (str.equals("Q"))
					return 10;
				else if (str.equals("K"))
					return 11;
				else if (str.equals("A"))
					return 12;
			}
			return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private CardParser parser = new CardParser();
		private String pip;

		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			parser.parse(value.toString());
			pip = parser.getPip();
			output.write(new Text(pip), one);
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,
				Context output) throws IOException, InterruptedException {
			int sum = 0;
			while (values.iterator().hasNext()) {
				sum += values.iterator().next().get();
			}
			output.write(key, new IntWritable(sum));
		}
	}

	public int run(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		Job job = Job.getInstance(getConf(), "Count by pip in deck of cards");

		job.setJarByClass(getClass());

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		// job.setGroupingComparatorClass(TextPair.Comparator.class);
		// job.setSortComparatorClass();
		// job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		// job.setNumReduceTasks(2);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new CardCountByPip(), args);
		System.exit(exitCode);

	}
}
