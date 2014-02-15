package com.connell.pi;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class provides the input to EstimatePiJob. The only real input
 * is in the number of digits the user has requested that the entire job
 * compute.
 */
public class EstimatorInputFormat implements InputFormat<IntWritable, NullWritable> {

  public static final String ESTIMATOR_DIGITS = "ESTIMATOR_DIGITS";

  /**
   * Used to set the number of digits we'll be computing in total
   */
  public static void setDigits(JobConf job, int digits) {
    job.setInt(ESTIMATOR_DIGITS, digits);
  }

  /**
   * Split the total number of digits into EstimatorInputSplits.
   * We'll create numSplits EstimatorInputSplits, and
   * evenly spread the range of digits to compute across them.
   */
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    int digits = job.getInt(ESTIMATOR_DIGITS, 100);
    // round to nearest chunk
    int splitSize = digits / numSplits;
    splitSize += digits % EstimatePiJob.DIGITS_PER_MAP;
    int i = 0;
    ArrayList<InputSplit> splits = new ArrayList<InputSplit>(numSplits);
    while (i < digits) {
      splits.add(new EstimatorInputSplit(i, i+splitSize));
      i += splitSize;
    }
    return splits.toArray(new InputSplit[0]);
  }

  /**
   * Create a RecordReader that will return the individual key/value
   * pairs contained in an InputSplit.
   */
  @Override
  public RecordReader<IntWritable, NullWritable> getRecordReader(InputSplit inputSplit, JobConf entries, Reporter reporter) throws IOException {
    EstimatorInputSplit realInputSplit = (EstimatorInputSplit)inputSplit;
    EstimatorInputReader reader = new EstimatorInputReader(realInputSplit.start(), realInputSplit.end());
    return reader;
  }
}
