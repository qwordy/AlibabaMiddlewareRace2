package com.alibaba.middleware.race;

import java.util.Collection;

/**
 * Created by yfy on 7/14/16.
 * Util
 */
public class Util {

  public static byte[] short2byte(int n) {
    byte[] b = new byte[2];
    b[1] = (byte) (n >> 8);
    b[0] = (byte) (n);
    return b;
  }

  public static int byte2short(byte[] b) {
    return byte2short(b, 0);
  }

  public static int byte2short(byte[] b, int offset) {
    return (((int) b[offset + 1] & 0xff) << 8)
        | ((int) b[offset] & 0xff);
  }

  public static byte[] int2byte(int n) {
    byte[] b = new byte[4];
    b[3] = (byte) (n >> 24);
    b[2] = (byte) (n >> 16);
    b[1] = (byte) (n >> 8);
    b[0] = (byte) (n);
    return b;
  }

  public static int byte2int(byte[] b) {
    return byte2int(b, 0);
  }

  public static int byte2int(byte[] b, int offset) {
    return (((int) b[offset + 3] & 0xff) << 24)
        | (((int) b[offset + 2] & 0xff) << 16)
        | (((int) b[offset + 1] & 0xff) << 8)
        | ((int) b[offset] & 0xff);
  }

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

  public static long byte2long(byte[] b) {
    return byte2long(b, 0);
  }

  public static long byte2long(byte[] b, int offset) {
    return (((long) b[offset + 7] & 0xff) << 56)
        | (((long) b[offset + 6] & 0xff) << 48)
        | (((long) b[offset + 5] & 0xff) << 40)
        | (((long) b[offset + 4] & 0xff) << 32)
        | (((long) b[offset + 3] & 0xff) << 24)
        | (((long) b[offset + 2] & 0xff) << 16)
        | (((long) b[offset + 1] & 0xff) << 8)
        | ((long) b[offset] & 0xff);
  }

  public static boolean bytesEqual(byte[] a, int aPos, byte[] b, int bPos, int len) {
    for (int i = 0; i < len; i++)
      if (a[aPos + i] != b[bPos + i])
        return false;
    return true;
  }

  public static String keysStr(Collection<String> keys) {
    if (keys == null)
      return "null";
    StringBuilder sb = new StringBuilder();
    for (String key : keys)
      sb.append(key).append(' ');
    return sb.toString();
  }

  public static int bytesHash(byte[] key) {
    int h = 0;
    for (byte b : key) {
      h = 31 * h + b;
    }
    return h;
  }

}
