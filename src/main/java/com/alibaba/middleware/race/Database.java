package com.alibaba.middleware.race;

import com.alibaba.middleware.race.hash.HashTable;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by yfy on 7/13/16.
 * Database
 */
public class Database {

  private List<String> orderFilesList;

  private HashTable orderHashTable;

  private final static byte[] orderidBytes =
      new byte[]{'o', 'r', 'd', 'e', 'r', 'i', 'd'};

  public Database(Collection<String> orderFiles,
                  Collection<String> buyerFiles,
                  Collection<String> goodFiles,
                  Collection<String> storeFolders) {
    orderFilesList = new ArrayList<>();
    for (String file : orderFiles)
      orderFilesList.add(file);
  }

  public void construct() throws Exception {
    buildOrder2OrderHash();
  }

  public void buildOrder2OrderHash() throws Exception {
    orderHashTable = new HashTable(orderFilesList, "order.hash", 8);
    for (int i = 0; i < orderFilesList.size(); i++)
      readOrderFile(orderFilesList.get(i), i);
  }

  private void readOrderFile(String filename, int fileId) throws Exception {
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

          if (expectedKey(key, keyLen, orderidBytes)) {
            long valueLong = Long.decode(new String(value, 0, valueLen));
            orderHashTable.add(valueLong, fileId, offset);
            orderHashTable.get(Util.long2byte(valueLong));
            status = 2;
          } else {
            keyLen = 0;
            status = 0;
          }
        } else if (b == '\n') {
          //print(key, keyLen, value, valueLen);
          //System.out.println("offset: " + offset);

          if (expectedKey(key, keyLen, orderidBytes)) {
            long valueLong = Long.decode(new String(value, 0, valueLen));
            orderHashTable.add(valueLong, fileId, offset);
            orderHashTable.get(Util.long2byte(valueLong));
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

  private boolean expectedKey(byte[] key, int keyLen, byte[] expectedKey) {
    if (keyLen != expectedKey.length)
      return false;
    for (int i = 0; i < keyLen; i++)
      if (key[i] != expectedKey[i])
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

  public ResultImpl queryOrder(long orderId, Collection<String> keys)
      throws Exception {

    Tuple orderTuple = orderHashTable.get(Util.long2byte(orderId));
    if (orderTuple == null)
      return null;

    ResultImpl result = new ResultImpl(orderTuple, keys);
    result.printOrderTuple();

    return result;
  }

//  public void readOrderFile2(String filename, int fileId) throws IOException {
//    orderHashTable = new HashTable("order.hash");
//
//    BufferedReader br = new BufferedReader(new FileReader(filename));
//    String line;
//    long fileOff = 0;  // offset in file
//    while ((line = br.readLine()) != null) {
//      int lineOff = 0, sepPos, tabPos = 0;
//      while (tabPos != -1) {
//        sepPos = line.indexOf(':', lineOff);  // pos of :
//        String key = line.substring(lineOff, sepPos);
//        tabPos = line.indexOf('\t', lineOff);  // pos of tab
//        String value;
//        if (tabPos == -1)
//          value = line.substring(sepPos + 1);
//        else
//          value = line.substring(sepPos + 1, tabPos);
//
//        System.out.println(key + ":" + value);
//
////        if (key.equals("orderid"))
////          orderHashTable.add(value, fileId, fileOff);
//
//        lineOff = tabPos + 1;
//      }
//      fileOff += line.length();
//
//      System.out.println(fileOff);
//    }
//  }

}
