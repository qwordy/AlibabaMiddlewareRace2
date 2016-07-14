package com.alibaba.middleware.race;

/**
 * Created by yfy on 7/14/16.
 * Util
 */
public class Util {
  public static byte[] long2byte(long n) {
    byte[] b = new byte[8];
    b[7] = (byte) (n >> 56);
    b[6] = (byte) (n >> 48);
    b[5] = (byte) (n >> 40);
    b[4] = (byte) (n >> 32);
    b[3] = (byte) (n >> 24);
    b[2] = (byte) (n >> 16);
    b[1] = (byte) (n >> 8);
    b[0] = (byte) (n);
    return b;
  }

  public static long byte2long(byte[] bb) {
    return ((((long) bb[7] & 0xff) << 56)
        | (((long) bb[6] & 0xff) << 48)
        | (((long) bb[5] & 0xff) << 40)
        | (((long) bb[4] & 0xff) << 32)
        | (((long) bb[3] & 0xff) << 24)
        | (((long) bb[2] & 0xff) << 16)
        | (((long) bb[1] & 0xff) << 8)
        | ((long) bb[0] & 0xff));
  }
}
