package com.connell.pi;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;

public class Runner {

  public static void main(String[] args) throws Exception {
    JobConf job = new JobConf(EstimatePiJob.class);
    job.setJobName("estimate-pi");
    job.setMapperClass(EstimatePiJob.EstimatePiMapper.class);
    job.setReducerClass(EstimatePiJob.EstimatePiReducer.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(BytesWritable.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    job.setInputFormat(EstimatorInputFormat.class);
    job.setOutputFormat(TextOutputFormat.class);

    job.setNumReduceTasks(1);

    EstimatorInputFormat.setDigits(job, Integer.valueOf(args[0]));

    FileOutputFormat.setOutputPath(job, new Path("/tmp/estimate-pi/results/" + System.currentTimeMillis()));

    JobClient.runJob(job);
  }
}
