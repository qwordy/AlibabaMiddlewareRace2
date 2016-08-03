package com.alibaba.middleware.race;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;

/**
 * Created by yfy on 7/14/16.
 * Util
 */
public class Util {

  public static void short2byte(int n, byte[] b, int off) {
    b[off + 1] = (byte) (n >> 8);
    b[off] = (byte) (n);
  }

  public static int byte2short(byte[] b, int offset) {
    return (((int) b[offset + 1] & 0xff) << 8)
        | ((int) b[offset] & 0xff);
  }

  public static int byte3Toint(byte[] b, int offset) {
    return (((int) b[offset + 2] & 0xff) << 16)
        | (((int) b[offset + 1] & 0xff) << 8)
        | ((int) b[offset] & 0xff);
  }

  public static void int2byte3(int n, byte[] b, int off) {
    b[off + 2] = (byte) (n >> 16);
    b[off + 1] = (byte) (n >> 8);
    b[off] = (byte) (n);
  }

  public static void int2byte(int n, byte[] b, int off) {
    b[off + 3] = (byte) (n >> 24);
    b[off + 2] = (byte) (n >> 16);
    b[off + 1] = (byte) (n >> 8);
    b[off] = (byte) (n);
  }

  public static int byte2int(byte[] b, int offset) {
    return (((int) b[offset + 3] & 0xff) << 24)
        | (((int) b[offset + 2] & 0xff) << 16)
        | (((int) b[offset + 1] & 0xff) << 8)
        | ((int) b[offset] & 0xff);
  }

  public static void long2byte5(long n, byte[] b, int off) {
    //b[7] = (byte) (n >> 56);
    //b[6] = (byte) (n >> 48);
    //b[5] = (byte) (n >> 40);
    b[off + 4] = (byte) (n >> 32);
    b[off + 3] = (byte) (n >> 24);
    b[off + 2] = (byte) (n >> 16);
    b[off + 1] = (byte) (n >> 8);
    b[off] = (byte) (n);
  }

  public static byte[] long2byte5(long n) {
    byte[] b = new byte[5];
//    b[7] = (byte) (n >> 56);
//    b[6] = (byte) (n >> 48);
//    b[5] = (byte) (n >> 40);
    b[4] = (byte) (n >> 32);
    b[3] = (byte) (n >> 24);
    b[2] = (byte) (n >> 16);
    b[1] = (byte) (n >> 8);
    b[0] = (byte) (n);
    return b;
  }

  public static void longToByte4(long n, byte[] b, int off) {
    b[off + 3] = (byte) (n >> 24);
    b[off + 2] = (byte) (n >> 16);
    b[off + 1] = (byte) (n >> 8);
    b[off] = (byte) (n);
  }

  public static long byte4ToLong(byte[] b, int offset) {
    return (((long) b[offset + 3] & 0xff) << 24)
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

  public static int bytesHash(byte[] key) {
    int h = 0;
    for (byte b : key)
      h = 31 * h + b;
    return h & 0x7fffffff;
  }

  public static int bytesHash(byte[] key, int len) {
    int h = 0;
    for (int i = 0; i < len; i++)
      h = 31 * h + key[i];
    return h & 0x7fffffff;
  }







  public static String keysStr(Collection<String> keys) {
    if (keys == null)
      return "null";
    StringBuilder sb = new StringBuilder();
    for (String key : keys)
      sb.append(key).append(' ');
    return sb.toString();
  }

  public static boolean keysContainKey(Collection<String> keys, byte[] key, int keyLen) {
    if (keys == null)
      return true;
    for (String queryKey : keys) {
      if (queryKey.length() == keyLen)
        if (bytesEqual(key, 0, queryKey.getBytes(), 0, keyLen))
          return true;
    }
    return false;
  }

  public static byte[] serialize(Object obj) throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(bos);
    oos.writeObject(obj);
    return bos.toByteArray();
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

  public static void long2byte(long n, byte[] b, int off) {
    b[off + 7] = (byte) (n >> 56);
    b[off + 6] = (byte) (n >> 48);
    b[off + 5] = (byte) (n >> 40);
    b[off + 4] = (byte) (n >> 32);
    b[off + 3] = (byte) (n >> 24);
    b[off + 2] = (byte) (n >> 16);
    b[off + 1] = (byte) (n >> 8);
    b[off] = (byte) (n);
  }


  public static byte[] int2byte(int n) {
    byte[] b = new byte[4];
    b[3] = (byte) (n >> 24);
    b[2] = (byte) (n >> 16);
    b[1] = (byte) (n >> 8);
    b[0] = (byte) (n);
    return b;
  }

  public static int byte2short(byte[] b) {
    return byte2short(b, 0);
  }

  public static byte[] short2byte(int n) {
    byte[] b = new byte[2];
    b[1] = (byte) (n >> 8);
    b[0] = (byte) (n);
    return b;
  }

  public static int byte2int(byte[] b) {
    return byte2int(b, 0);
  }

}
