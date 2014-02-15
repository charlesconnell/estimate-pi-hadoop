package com.connell.pi;

import org.apache.hadoop.mapred.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * One "piece" of input to give to a map task. In our
 * case, this is a range of numbers. EstimatePiMapper will
 * be asked to run many times for each InputSplit, to
 * cover the entire range.
 */
public class EstimatorInputSplit implements InputSplit {

  private int start;
  private int end;

  public EstimatorInputSplit() {
    super();
  }

  public EstimatorInputSplit(int start, int end) {
    super();
    this.start = start;
    this.end = end;
  }

  public int start() {
    return start;
  }

  public int end() {
    return end;
  }

  @Override
  public long getLength() throws IOException {
    return end - start;
  }

  @Override
  public String[] getLocations() throws IOException {
    return new String[0];
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(start);
    dataOutput.writeInt(end);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    start = dataInput.readInt();
    end = dataInput.readInt();
  }
}
