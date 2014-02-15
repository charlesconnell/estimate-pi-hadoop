package com.connell.utils;

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

  public static double frac (double n) {
    return n - Math.floor(n);
  }


}