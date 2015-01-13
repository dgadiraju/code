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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import demo.cards.parsers.CardParser;

import java.io.IOException;

// This map reduce program will copy data partitioned by pip
public class DistributeCardsByPip extends Configured implements Tool {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private CardParser parser = new CardParser();
		private String pip;

		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			parser.parse(value.toString());
			pip = parser.getPip();
			output.write(new Text(pip), new Text(parser.getColorAndSuit()));
		}
	}

	public static class Reduce extends Reducer<Text, Text, NullWritable, Text> {

		private MultipleOutputs<NullWritable, Text> multipleOutputs;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);

		}

		public void reduce(Text key, Iterable<Text> values, Context output)
				throws IOException, InterruptedException {
			while (values.iterator().hasNext()) {
				String basePath = String.format("%s/part",
						"pip=" + key.toString());
				multipleOutputs.write(NullWritable.get(), values.iterator()
						.next(), basePath);
			}
		}
	}

	public int run(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		Job job = Job.getInstance(getConf(),
				"Distribute by Pip in deck of cards");

		job.setJarByClass(getClass());

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		// job.setGroupingComparatorClass(TextPair.Comparator.class);
		// job.setSortComparatorClass();
		// job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setNumReduceTasks(4);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new DistributeCardsByPip(), args);
		System.exit(exitCode);

	}
}
