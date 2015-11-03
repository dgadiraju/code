package nyse.topthreestocksbyvolume;

import java.io.PrintStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import nyse.comparators.LongPairPrimitiveGroupingComparator;
import nyse.comparators.LongPairPrimitiveSortingComparator;
import nyse.keyvalues.LongPairPrimitive;
import nyse.partitioners.FirstKeyLongPairPartitioner;

public class TopThreeStocksByVolumePerDayCompressDriver extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		
		GenericOptionsParser parser = new GenericOptionsParser(conf, arg0);
		String[] args = parser.getRemainingArgs();
		
		conf.setBoolean("mapreduce.compress.map.output", true);
		conf.setClass("mapreduce.map.output.compress.codec", 
				SnappyCodec.class, CompressionCodec.class);
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(getClass());
		
		FileSystem fs = FileSystem.get(URI.create(args[0]), conf);
		Path path = new Path(args[0] + args[1]); //arg0[1] NYSE_201[2-3]
		
		FileStatus[] status = fs.globStatus(path);
		Path[] paths = FileUtil.stat2Paths(status);
		for(Path p : paths) {
			System.out.println(p.toString());
			FileInputFormat.addInputPath(job, p);
		}
		
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(TopThreeStocksByVolumePerDayMapper.class);
		job.setMapOutputKeyClass(LongPairPrimitive.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setPartitionerClass(FirstKeyLongPairPartitioner.class);
		job.setGroupingComparatorClass(LongPairPrimitiveGroupingComparator.class);
		job.setSortComparatorClass(LongPairPrimitiveSortingComparator.class);
		job.setNumReduceTasks(6);
		
		job.setReducerClass(TopThreeStocksByVolumePerDayReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new TopThreeStocksByVolumePerDayCompressDriver(), args));
	}

}
