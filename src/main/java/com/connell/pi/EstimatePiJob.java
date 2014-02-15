package com.connell.pi;

import com.connell.utils.Arithmetic;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Iterator;

/**
 * Estimate the value of pi using the BBP method.
 *
 * Based on this paper:
 * http://crd.lbl.gov/~dhbailey/dhbpapers/bbp-alg.pdf
 * by David Bailey (one of the B's in BBP).
 */
public class EstimatePiJob {

  public static final int DIGITS_PER_MAP = 5;

  /**
   * The mapper will do all the computation.
   */
  public static class EstimatePiMapper extends MapReduceBase implements Mapper<IntWritable, NullWritable, IntWritable, BytesWritable> {

    private BytesWritable digitsWritable = new BytesWritable();

    private static final int[] js = {1, 4, 5, 6};

    /**
     * Implements equation 5 (and by extension, equation 7)
     * in the above mentioned paper.
     * This method computes a number equal to the fractional
     * part of (16^d)*pi. This number can be used to determine
     * the digits of pi starting at d, in hexadecimal.
     *
     * @param d The number of digits after the decimal point at which to
     *          start computing the digits of pi.
     * @return The fractional part of (16^d)*pi
     */
    public static double pi16d (int d) {
      double jSums[] = {0, 0, 0, 0};
      int i = 0;
      for (int j : js) {
        double part1 = 0;
        double part2 = 0;
        for (int k = 0; k <= d; k++) {
          part1 += ((double)Arithmetic.binExpMod(16, d-k, (8*k)+j) / (double)((8*k)+j));
        }
        for (int k = d+1; k <= d+10000; k++) {
          part2 += Math.pow(16, d - k) / (double)((8*k) + j);
        }
        jSums[i++] = Arithmetic.frac(Arithmetic.frac(part1) + part2);
      }

      return Arithmetic.frac((4 * Arithmetic.frac(jSums[0])) -
                             (2 * Arithmetic.frac(jSums[1])) -
                                  Arithmetic.frac(jSums[2]) -
                                  Arithmetic.frac(jSums[3]));
    }

    /**
     * Takes in the fractional part of (16^d)*pi, and generates
     * the digits of pi, starting at d, in hexadecimal. This implements
     * the paragraph below question 8 in the above mentioned paper.
     * @param pi16 This is the fractional part of (16^d)*pi.
     * @return A byte array whose contents are equal to the value
     * of the hex digits. (Their actual value, not ASCII representations).
     */
    public static byte[] hexDigits (double pi16) {
      byte[] digits = new byte[DIGITS_PER_MAP];
      for (int i = 0; i < digits.length; i++) {
        pi16 *= 16;
        double digitVal = Math.floor(pi16);
        byte digit = Double.valueOf(digitVal).byteValue();
        digits[i] = digit;
        pi16 -= Math.floor(pi16);
      }
      return digits;
    }

    /**
     * Compute DIGITS_PER_MAP hex digits of pi, starting at
     * d digits after the decimal point, where d is in the key
     * @param key d, how many digits after the decimal point
     * @throws IOException
     */
    @Override
    public void map(IntWritable key, NullWritable value, OutputCollector<IntWritable, BytesWritable> output, Reporter reporter) throws IOException {
      double pi16d = pi16d(key.get());
      digitsWritable.set(hexDigits(pi16d), 0, EstimatePiJob.DIGITS_PER_MAP);
      output.collect(key, digitsWritable);
    }

    @Override
    public void configure(JobConf job) { }

    @Override
    public void close() throws IOException { }

  }

  /**
   * The reducer simply outputs the results. This could probably be moved into the mapper,
   * but there is little performance penalty in this case, so it is not important.
   */
  public static class EstimatePiReducer extends MapReduceBase implements Reducer<IntWritable, BytesWritable, IntWritable, Text> {

    private final Text result = new Text();

    /**
     * Write the results of the mapper as hex digits.
     */
    @Override
    public void reduce(IntWritable key, Iterator<BytesWritable> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
      StringBuilder str = new StringBuilder();

      while (values.hasNext()) {
        byte[] digits = values.next().getBytes();
        for (int i = 0; i < DIGITS_PER_MAP; i++) {
          str.append(String.format("%X", digits[i]));
        }
      }

      result.set(str.toString());

      output.collect(key, result);
    }

    @Override
    public void close() throws IOException { }

    @Override
    public void configure(JobConf entries) { }

  }


}
