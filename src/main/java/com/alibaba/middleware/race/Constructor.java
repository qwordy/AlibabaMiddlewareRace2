package com.alibaba.middleware.race;

import com.alibaba.middleware.race.hash.HashTable;

import java.io.*;

/**
 * Created by yfy on 7/13/16.
 * Constructor
 */
public class Constructor {

  private HashTable orderHashTable;

  private final static byte[] orderidBytes =
      new byte[]{'o', 'r', 'd', 'e', 'r', 'i', 'd'};

  public Constructor() {
  }

  public void readOrderFile(String filename, int fileId) throws IOException {
    orderHashTable = new HashTable("order.hash");

    BufferedInputStream bis = new BufferedInputStream(new FileInputStream(filename));
    int b, keyLen = 0, valueLen = 0;
    long offset = 0, count = 0;
    // 0 for read key, 1 for read value, 2 for skip line
    int status = 0;
    byte[] key = new byte[256];
    byte[] value = new byte[65536];
    while ((b = bis.read()) != -1) {
      count++;
      if (status == 0) {
        if (b == ':') {
          valueLen = 0;
          status = 1;
        } else {
          key[keyLen++] = (byte) b;
        }
      } else if (status == 1) {
        if (b == '\t') {
          //print(key, keyLen, value, valueLen);

          if (bytesEqual(key, keyLen, orderidBytes)) {
            long valueLong = Long.decode(new String(value, 0, valueLen));
            orderHashTable.add(valueLong, fileId, offset);
            status = 2;
          } else {
            keyLen = 0;
            status = 0;
          }
        } else if (b == '\n') {
          //print(key, keyLen, value, valueLen);
          //System.out.println("offset: " + offset);

          if (bytesEqual(key, keyLen, orderidBytes)) {
            long valueLong = Long.decode(new String(value, 0, valueLen));
            orderHashTable.add(valueLong, fileId, offset);
          }

          offset = count;
          keyLen = 0;
          status = 0;
        } else {
          value[valueLen++] = (byte) b;
        }
      } else {
        if (b == '\n') {
          //System.out.println("offset: " + offset);

          offset = count;
          keyLen = 0;
          status = 0;
        }
      }
    }
  }

  private boolean bytesEqual(byte[] a, int aLen, byte[] b) {
    if (aLen != b.length)
      return false;
    for (int i = 0; i < aLen; i++)
      if (a[i] != b[i])
        return false;
    return true;
  }

  private void print(byte[] key, int keyLen, byte[] value, int valueLen) {
    for (int i = 0; i < keyLen; i++)
      System.out.print((char)key[i]);
    System.out.print(':');
    for (int i = 0; i < valueLen; i++)
      System.out.print((char)value[i]);
    System.out.print('\t');
  }

  public void readOrderFile2(String filename, int fileId) throws IOException {
    orderHashTable = new HashTable("order.hash");

    BufferedReader br = new BufferedReader(new FileReader(filename));
    String line;
    long fileOff = 0;  // offset in file
    while ((line = br.readLine()) != null) {
      int lineOff = 0, sepPos, tabPos = 0;
      while (tabPos != -1) {
        sepPos = line.indexOf(':', lineOff);  // pos of :
        String key = line.substring(lineOff, sepPos);
        tabPos = line.indexOf('\t', lineOff);  // pos of tab
        String value;
        if (tabPos == -1)
          value = line.substring(sepPos + 1);
        else
          value = line.substring(sepPos + 1, tabPos);

        System.out.println(key + ":" + value);

//        if (key.equals("orderid"))
//          orderHashTable.add(value, fileId, fileOff);

        lineOff = tabPos + 1;
      }
      fileOff += line.length();

      System.out.println(fileOff);
    }
  }

}
