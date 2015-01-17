package demo.cards.drivers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import demo.cards.mappers.RecordMapper;
import demo.cards.reducers.NoKeyRecordCountReducer;

/* This program can be used for 
 * select count(1) from <table_name>;
 * Steps
 * 1. Develop mapper function
 * 2. Develop reducer function
 * 3. Job configuration
 */

public class RowCountCombinedFileInputFormat extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new RowCountCombinedFileInputFormat(),
				args);
		System.exit(exitCode);

	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();

		Job job = Job
				.getInstance(conf, "Row Count using combined input format");

		job.setJarByClass(getClass());

		FileInputFormat.setInputPaths(job, new Path(args[0]));

		// Setting input format class to reduce number of mappers 
		// in case of there are many small files significantly less than
		// block size
		job.setInputFormatClass(CombineTextInputFormat.class);
		CombineTextInputFormat.setMaxInputSplitSize(job, 128000000);
		
		// Custom mapper (RecordMapper) to assign 1 for each record
		// Input to mapper <Lineoffset as key, entire line as value>
		// Default behavior of default input format TextInputFormat
		job.setMapperClass(RecordMapper.class);
		// Output from mapper
		// <count, 1>
		// <count, 1> so on

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// Built-in reducer
		// Input to reducer <count, {1, 1, 1, ...}>
		// job.setReducerClass(IntSumReducer.class);
		// Output from reducer <count, Number of records>

		// Custom reducer
		// If you do not want to see "count" as part of output
		// and just see the record count as in select count query
		// job.setCombinerClass(NoKeyRecordCountReducer.class);
		job.setReducerClass(NoKeyRecordCountReducer.class);
		// Output from reducer <Number of records>

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(IntWritable.class);

		// We are not setting output format class and hence uses default
		// (TextOutputFormat)

		// job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

}
