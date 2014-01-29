package com.connell.pi;

import com.connell.utils.Arithmetic;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Iterator;

public class EstimatePiJob {

  public static final int DIGITS_PER_MAP = 5;

  private static final MathContext mc = new MathContext(DIGITS_PER_MAP*2, RoundingMode.HALF_EVEN);
  private static final BigDecimal TWO = new BigDecimal(2, mc);
  private static final BigDecimal FOUR = new BigDecimal(4, mc);
  private static final BigDecimal SIXTEEN = new BigDecimal(16, mc);

  public static class EstimatePiMapper extends MapReduceBase implements Mapper<IntWritable, NullWritable, IntWritable, BytesWritable> {

    private BytesWritable digitsWritable = new BytesWritable();

    private static final int[] js = {1, 4, 5, 6};

    public static double log2_2d(int d) {
      double part1 = 0;
      double part2 = 0;
      for (int k = 1; k <= d; k++) {
        part1 += ((double) Arithmetic.binExpMod(2, d - k, k) / (double)k);
      }
      for (int k = d+1; k <= d+100; k++) {
        part2 += Math.pow(2, d - k) / (double)k;
      }
      return Arithmetic.frac(Arithmetic.frac(part1) + part2);
    }

    public static BigDecimal pi16d_big (int d) {
      BigDecimal jSums[] = {BigDecimal.ZERO,BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO};
      int i = 0;
      for (int j : js) {
        BigDecimal part1 = BigDecimal.ZERO;
        BigDecimal part2 = BigDecimal.ZERO;
        for (int k = 0; k <= d; k++) {
          part1 = part1.add(new BigDecimal(Arithmetic.binExpMod(16, d - k, (8 * k) + j)).divide(new BigDecimal((8 * k) + j), mc), mc);
        }
        for (int k = d+1; k <= d+100; k++) {
          part2 = part2.add(new BigDecimal(Math.pow(16, d - k)).divide(new BigDecimal((8 * k) + j), mc), mc);
        }
        jSums[i++] = Arithmetic.frac(Arithmetic.frac(part1).add(part2));
      }

      return Arithmetic.frac(FOUR.multiply(Arithmetic.frac(jSums[0]), mc)
              .subtract(TWO.multiply(Arithmetic.frac(jSums[1]), mc), mc)
              .subtract(Arithmetic.frac(jSums[2]), mc)
              .subtract(Arithmetic.frac(jSums[3]), mc));
    }

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

    public static byte[] hexDigits (BigDecimal pi16) {
      byte[] digits = new byte[DIGITS_PER_MAP];
      for (int i = 0; i < digits.length; i++) {
        pi16 = SIXTEEN.multiply(pi16, mc);
        BigDecimal digitVal = pi16.setScale(0, RoundingMode.FLOOR);
        byte digit = digitVal.byteValue();
        digits[i] = digit;
        pi16 = pi16.subtract(pi16.setScale(0, RoundingMode.FLOOR));
      }
      return digits;
    }

    @Override
    public void map(IntWritable key, NullWritable value, OutputCollector<IntWritable, BytesWritable> output, Reporter reporter) throws IOException {
      double pi16d = pi16d(key.get());
      digitsWritable.set(hexDigits(pi16d), 0, EstimatePiJob.DIGITS_PER_MAP);
      output.collect(key, digitsWritable);
    }

    @Override
    public void configure(JobConf job) {
    }

    @Override
    public void close() throws IOException {

    }
  }

  public static class EstimatePiReducer extends MapReduceBase implements Reducer<IntWritable, BytesWritable, IntWritable, Text> {

    private final Text result = new Text();

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
    public void close() throws IOException {

    }

    @Override
    public void configure(JobConf entries) {

    }
  }


}
