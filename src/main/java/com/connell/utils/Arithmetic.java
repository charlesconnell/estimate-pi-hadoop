package com.connell.utils;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class Arithmetic {

  public static long binExpMod (int b, int exp, int k) {
    if (exp == 0) {
      return 1;
    }

    long t = Integer.highestOneBit(exp);
    long n = exp;
    long r = 1;
    while (true) {
      if (n >= t) {
        r = (b*r) % k;
        n = n - t;
      }
      t = t / 2;
      if (t >= 1) {
        r = (r * r) % k;
      } else {
        break;
      }
    }
    return r;
  }


  public static long binExp (int b, int exp) {
    if (exp == 0) {
      return 1;
    }

    long t = Integer.highestOneBit(exp);
    long n = exp;
    long r = 1;
    while (true) {
      if (n >= t) {
        r = (b*r);
        n = n - t;
      }
      t = t / 2;
      if (t >= 1) {
        r = (r * r);
      } else {
        break;
      }
    }
    return r;
  }

  public static double frac (double n) {
    return n - Math.floor(n);
  }

  public static BigDecimal frac (BigDecimal n) {
    BigDecimal floor = n.setScale(0, RoundingMode.FLOOR);
    return n.subtract(floor);
  }

}