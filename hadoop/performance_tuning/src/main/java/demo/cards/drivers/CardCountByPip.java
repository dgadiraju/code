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

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private CardParser parser = new CardParser();
        private String pip;

        public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {
            parser.parse(value.toString());
            pip = parser.getPip();
            output.write(new Text(pip), one);
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context output) throws IOException, InterruptedException {
            int sum = 0;
            while (values.iterator().hasNext()) {
                sum += values.iterator().next().get();
            }
            output.write(key, new IntWritable(sum));
        }
    }

    public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(getConf(), "Count by pip in deck of cards");

        job.setJarByClass(getClass());

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
//        job.setGroupingComparatorClass(TextPair.Comparator.class);
//        job.setSortComparatorClass();
//        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setNumReduceTasks(2);

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
