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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class CompressionCopy extends Configured implements Tool {

    public static class Map extends Mapper<LongWritable, Text, Text, NullWritable> {

        public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {
            output.write(value, NullWritable.get());
        }
    }


    public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = getConf();
        conf.setBoolean("mapred.compress.map.output", true);
        conf.setClass("mapred.map.output.compression.codec", GzipCodec.class,
                CompressionCodec.class);
        // Above code for map output compression
//        In the old API, there are convenience methods on the JobConf object for doing the same thing:
//        conf.setCompressMapOutput(true);
//        conf.setMapOutputCompressorClass(GzipCodec.class);
        Job job = Job.getInstance(conf, "Compression");

        job.setJarByClass(getClass());

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Below will compress the output to the files (Reduce output)
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);


        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CompressionCopy(), args);
        System.exit(exitCode);

    }
}
