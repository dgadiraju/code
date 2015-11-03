package nyse.stockcompanyjoin.distcache;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import nyse.comparators.LongPairPrimitiveGroupingComparator;
import nyse.comparators.LongPairPrimitiveSortingComparator;
import nyse.keyvalues.LongPairPrimitive;
import nyse.keyvalues.TextPair;
import nyse.partitioners.FirstKeyLongPairPartitioner;

public class StockCompanyJoinDistCacheDriver extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(getClass());
		
		FileSystem fs = FileSystem.get(URI.create(arg0[0]), conf);
		Path path = new Path(arg0[0] + arg0[1]); //arg0[1] NYSE_201[2-3]
		
		FileStatus[] status = fs.globStatus(path);
		Path[] paths = FileUtil.stat2Paths(status);
		for(Path p : paths) {
			System.out.println(p.toString());
			FileInputFormat.addInputPath(job, p);
		}
		
		job.setInputFormatClass(CombineTextInputFormat.class);
		
		job.setMapperClass(StockCompanyJoinDistCacheMapper.class);
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setNumReduceTasks(0);
				
		FileOutputFormat.setOutputPath(job, new Path(arg0[2]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new StockCompanyJoinDistCacheDriver(), args));
	}

}
