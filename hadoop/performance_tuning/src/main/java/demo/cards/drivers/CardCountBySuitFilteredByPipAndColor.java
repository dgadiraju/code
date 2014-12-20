package demo.cards.drivers;

/**
 * Created with IntelliJ IDEA.
 * User: cloudera
 * Date: 9/22/13
 * Time: 1:27 PM
 * To change this template use File | Settings | File Templates.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import demo.cards.parsers.CardParser;

import java.io.IOException;

/* This will demonstrate how we should design and develop efficient map reduce programs
 * select suit, count(1) from deck where pip='J' and color='RED' group by suit;
 * You will learn using fs package to eliminate unnecessary directories, significance
 * of setup method in map reduce life cycle as well as passing parameters from command line to 
 * map/reduce functions.
 * 
 * 1) Understand requirements
 * 2) Design directories and files (partition by pip)
 * 3) Eliminate unnecessary directories as part of job configuration using org.apache.hadoop.fs
 * 4) Pass on color as parameter to map function and filter out all non RED colored cards
 * 5) Review counters
 * 
 * Usage: hadoop jar performance_tuning.jar 
 * demo.cards.drivers.CardCountBySuitFilteredByPipAndColor 
 * /user/hduser/cards /user/hduser/output.cards 
 * J RED
 */

public class CardCountBySuitFilteredByPipAndColor extends Configured implements
		Tool {

	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private CardParser parser = new CardParser();
		private String suit;

		private Configuration jobconf = null;

		public void setup(Context context) throws IOException,
				InterruptedException {
			this.jobconf = context.getConfiguration();
		}

		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			parser.parse(value.toString());

			String param = jobconf.get("param.color");

			if (parser.getColor().equals(param)) {
				suit = parser.getSuit();
				output.write(new Text(suit), one);
			}
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
		Configuration conf = getConf();

		conf.set("param.color", args[3]);

		Job job = Job.getInstance(conf, "select suit, count(1) from deck "
				+ "where pip='J' and color='RED' group by suit");

		job.setJarByClass(getClass());

		String pip = args[2];
		Path inputPath = new Path(args[0] + "/" + "pip=" + pip);

		FileInputFormat.setInputPaths(job, inputPath);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		// job.setGroupingComparatorClass(TextPair.Comparator.class);
		// job.setSortComparatorClass();
		// job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setNumReduceTasks(2);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(
				new CardCountBySuitFilteredByPipAndColor(), args);
		System.exit(exitCode);

	}
}
