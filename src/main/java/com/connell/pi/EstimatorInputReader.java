package com.connell.pi;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

/**
 * For a particular range of digits, provide the digits that
 * EstimatePiJob will be asked to work on.
 */
public class EstimatorInputReader implements RecordReader<IntWritable, NullWritable> {

  private int currentDigit;
  private int start;
  private int end;

  public EstimatorInputReader(int start, int end) {
    this.currentDigit = start;
    this.start = start;
    this.end = end;
  }

  @Override
  public float getProgress() throws IOException {
    return (float)(currentDigit - start) / (float)(end - start);
  }

  /**
   * Give the next digit to compute as the key.
   * Digits are not sequential, but skip by DIGITS_PER_MAP.
   * Whatever we do here is what EstimatePiMapper will get
   * as input.
   */
  @Override
  public boolean next(IntWritable key, NullWritable value) throws IOException {
    if (currentDigit < end) {
      key.set(currentDigit);
      currentDigit += EstimatePiJob.DIGITS_PER_MAP;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public IntWritable createKey() {
    return new IntWritable();
  }

  @Override
  public NullWritable createValue() {
    return NullWritable.get();
  }

  @Override
  public long getPos() throws IOException {
    return currentDigit - start;
  }

  @Override
  public void close() throws IOException {

  }
}
