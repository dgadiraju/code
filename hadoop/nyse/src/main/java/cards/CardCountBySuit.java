package cards;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CardCountBySuit extends Configured implements Tool{
	
	private static class CardCountBySuitMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		public void map(LongWritable lineOffset, Text record, Context context) throws IOException, InterruptedException {
			context.write(new Text(record.toString().split("\\|")[1]), new LongWritable(1));
		}
	}

	public int run(String[] arg0) throws Exception {
		
		Job job = Job.getInstance(getConf());
		
		job.setJarByClass(getClass());
		
		FileInputFormat.setInputPaths(job, new Path(arg0[0]));
		
		job.setMapperClass(CardCountBySuitMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setPartitionerClass(HashPartitioner.class);
		job.setNumReduceTasks(2);
		//job.setCombinerClass(LongSumReducer.class);
		job.setReducerClass(LongSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		// TODO Auto-generated method stub
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new CardCountBySuit(), args));
	}

}
