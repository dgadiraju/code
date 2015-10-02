package nyse.avgstockvolpermonth;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import nyse.keyvalues.LongPair;
import nyse.keyvalues.TextPair;

public class AvgStockVolumePerMonthDriver extends Configured implements Tool {

	public static class MonthPartitioner extends Partitioner<TextPair, LongPair> {

		@Override
		public int getPartition(TextPair key, LongPair value, int numPartitions) {
			// TODO Auto-generated method stub
			String tradeMonth = key.getFirst().toString().replace("-", "");
			return new Integer(tradeMonth) % numPartitions;
		}

	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf);
		job.setJarByClass(getClass());

		FileSystem fs = FileSystem.get(URI.create(arg0[0]), conf);
		Path path = new Path(arg0[0] + arg0[1]); // arg0[1] NYSE_201[2-3]

		FileStatus[] status = fs.globStatus(path);
		Path[] paths = FileUtil.stat2Paths(status);
		for (Path p : paths) {
			System.out.println(p.toString());
			FileInputFormat.addInputPath(job, p);
		}

		job.setInputFormatClass(CombineTextInputFormat.class);
		CombineTextInputFormat.setMaxInputSplitSize(job, 32000000);

		job.setMapperClass(AvgStockVolPerMonthMapper.class);

		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(LongPair.class);

		// job.setPartitionerClass(MonthPartitioner.class);
		job.setPartitionerClass(TotalOrderPartitioner.class);
		Path inputDir = new Path(arg0[3]);
		Path partitionFile = new Path(inputDir, "partitioning");
		TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionFile);

		double pcnt = 0.10;
		int numReduceTasks = 4;
		int numSamples = 10000;
		int maxSplits = numReduceTasks - 1;
		if (0 >= maxSplits)
			maxSplits = Integer.MAX_VALUE;

		InputSampler.Sampler<TextPair, LongPair> sampler = new InputSampler.RandomSampler<TextPair, LongPair>(pcnt, numSamples);
		InputSampler.writePartitionFile(job, sampler);

//		job.setCombinerClass(AvgStockVolPerMonthCombiner.class);
		job.setReducerClass(AvgStockVolPerMonthReducer.class);

		job.setNumReduceTasks(numReduceTasks );

		job.setOutputKeyClass(TextPair.class);
		job.setOutputValueClass(LongPair.class);

		FileOutputFormat.setOutputPath(job, new Path(arg0[2]));

		// TODO Auto-generated method stub
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new AvgStockVolumePerMonthDriver(), args));
	}
}
