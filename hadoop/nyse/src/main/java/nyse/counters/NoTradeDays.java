package nyse.counters;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.util.*;

import nyse.topthreestocksbyvolume.TopThreeStocksByVolumePerDayReducer;

public class NoTradeDays extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    String jobID = args[0];
    Cluster cluster = new Cluster(getConf());
    Job job = cluster.getJob(JobID.forName(jobID));
    if (job == null) {
      System.err.printf("No job with ID %s found.\n", jobID);
      return -1;
    }
    if (!job.isComplete()) {
      System.err.printf("Job %s is not complete.\n", jobID);
      return -1;
    }

    Counters counters = job.getCounters();
    long missing = counters.findCounter(
    		TopThreeStocksByVolumePerDayReducer.NOTRADEDAYS.NOTRADE).getValue();
    long total = counters.findCounter(TaskCounter.REDUCE_INPUT_GROUPS).getValue();

    System.out.printf("Days on which no trade happened: %.2f%%\n",
        100.0 * missing / total);
    return 0;
  }
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new NoTradeDays(), args);
    System.exit(exitCode);
  }
}