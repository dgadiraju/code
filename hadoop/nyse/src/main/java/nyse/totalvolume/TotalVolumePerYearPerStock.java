package nyse.totalvolume;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import nyse.parsers.NYSEParser;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

public class TotalVolumePerYearPerStock extends Configured implements Tool {

	private static class TotalVolumePerYearPerStockMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		String stockTicker;

		private Configuration conf = null;
		NYSEParser parser = new NYSEParser();

		protected void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			stockTicker = conf.get("filter.by.ticker");
		}

		public void map(LongWritable lineOffset, Text record, Context output) throws IOException, InterruptedException {
			parser.parse(record.toString());

			if (parser.getStockTicker().equals(stockTicker))
				output.write(new Text(stockTicker), new LongWritable(parser.getVolume()));
		}

	}

	public int run(String[] arg0) throws Exception {

		Configuration conf = getConf();
		Job job = Job.getInstance(conf);
		
		FileSystem fs = FileSystem.get(URI.create(arg0[0]), conf);
		
	    FileStatus[] fileStatus = fs.listStatus(new Path(arg0[0]));
	    
	    Path[] paths = FileUtil.stat2Paths(fileStatus);
	    
	    for(Path path : paths)
	    {
	      System.out.println(path);
	    }
	    
		job.setJarByClass(getClass());

		String filterByYear = conf.get("filter.by.year");

		System.out.println(filterByYear);

		Path path = new Path(arg0[0] + "/NYSE_" + filterByYear);
		FileInputFormat.setInputPaths(job, path);

		job.setInputFormatClass(CombineTextInputFormat.class);
		job.setMapperClass(TotalVolumePerYearPerStockMapper.class);

		// job.setMapOutputKeyClass(Text.class);
		// job.setMapOutputValueClass(LongWritable.class);

		job.setReducerClass(LongSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setNumReduceTasks(1);

		job.setOutputFormatClass(TextOutputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(new TotalVolumePerYearPerStock(), args));
	}

}
