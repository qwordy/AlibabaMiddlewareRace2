package com.alibaba.middleware.race;

import com.alibaba.middleware.race.kvDealer.BuyerKvDealer;
import com.alibaba.middleware.race.kvDealer.GoodKvDealer;
import com.alibaba.middleware.race.kvDealer.IKvDealer;
import com.alibaba.middleware.race.kvDealer.OrderKvDealer;

import java.io.*;
import java.util.*;

/**
 * Created by yfy on 7/13/16.
 * Database
 */
public class Database {

  private List<String> orderFilesList, goodFilesList, buyerFilesList;

  private HashTable orderHashTable, goodHashTable, buyerHashTable;

  private HashTable buyer2OrderHashTable, good2OrderHashTable;

  private static TupleCreatetimeComparator tupleCreatetimeComparator;

  private static TupleOrderidComparator tupleOrderidComparator;

  public Database(Collection<String> orderFiles,
                  Collection<String> buyerFiles,
                  Collection<String> goodFiles,
                  Collection<String> storeFolders) {

    tupleCreatetimeComparator = new TupleCreatetimeComparator();
    tupleOrderidComparator = new TupleOrderidComparator();

    orderFilesList = new ArrayList<>();
    for (String file : orderFiles)
      orderFilesList.add(file);

    goodFilesList = new ArrayList<>();
    for (String file : goodFiles)
      goodFilesList.add(file);

    buyerFilesList = new ArrayList<>();
    for (String file : buyerFiles)
      buyerFilesList.add(file);
  }

  public void construct() throws Exception {
    buildOrder2OrderHash();
    buildGood2GoodHash();
    buildBuyer2BuyerHash();
  }

  private void buildOrder2OrderHash() throws Exception {
    orderHashTable =
        new HashTable(orderFilesList, "order.hash", 100, 8, false, 0);
    buyer2OrderHashTable =
        new HashTable(orderFilesList, "buyer2Order.hash", 100, 0, true, 8);
    good2OrderHashTable =
        new HashTable(orderFilesList, "good2Order.hash", 100, 0, true, 8);
    OrderKvDealer dealer = new OrderKvDealer(
        orderHashTable, buyer2OrderHashTable, good2OrderHashTable);
    for (int i = 0; i < orderFilesList.size(); i++) {
      dealer.setFileId(i);
      readDataFile(orderFilesList.get(i), dealer);
    }
  }

  private void buildGood2GoodHash() throws Exception {
    goodHashTable = new HashTable(goodFilesList, "good.hash", 100, 0, false, 0);
    HashTable.goodHashTable = goodHashTable;
    GoodKvDealer dealer = new GoodKvDealer(goodHashTable);
    for (int i = 0; i < goodFilesList.size(); i++) {
      dealer.setFileId(i);
      readDataFile(goodFilesList.get(i), dealer);
    }
  }

  private void buildBuyer2BuyerHash() throws Exception {
    buyerHashTable = new HashTable(buyerFilesList, "buyer.hash", 100, 0, false, 0);
    HashTable.buyerHashTable = buyerHashTable;
    BuyerKvDealer dealer = new BuyerKvDealer(buyerHashTable);
    for (int i = 0; i < buyerFilesList.size(); i++) {
      dealer.setFileId(i);
      readDataFile(buyerFilesList.get(i), dealer);
    }
  }

  private void readDataFile(String filename, IKvDealer dealer) throws Exception {
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
          int code = dealer.deal(key, keyLen, value, valueLen, offset);
          if (code == 2)
            status = 2;
          else
            status = keyLen = 0;
        } else if (b == '\n') {
          dealer.deal(key, keyLen, value, valueLen, offset);
          offset = count;
          status = keyLen = 0;
        } else {
          value[valueLen++] = (byte) b;
        }
      } else { // status == 2
        if (b == '\n') {
          offset = count;
          status = keyLen = 0;
        }
      }
    }
    dealer.deal(key, keyLen, value, valueLen, offset);
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
    //result.printOrderTuple();

    return result;
  }

  public Iterator<OrderSystem.Result> queryOrdersByBuyer(
      long startTime, long endTime, String buyerid) throws Exception {

    TupleFilter filter = new TupleFilter(startTime, endTime);
    List<Tuple> tupleList =  buyer2OrderHashTable.getMulti(buyerid.getBytes(), filter);
    Collections.sort(tupleList, tupleCreatetimeComparator);
    List<OrderSystem.Result> resultList = new ArrayList<>();
    for (Tuple tuple : tupleList)
      resultList.add(new ResultImpl(tuple, null));
    return resultList.iterator();
  }

  public Iterator<OrderSystem.Result> queryOrdersBySaler(
      String goodid, Collection<String> keys) throws Exception {

    List<Tuple> tupleList = good2OrderHashTable.getMulti(goodid.getBytes(), null);
    Collections.sort(tupleList, tupleOrderidComparator);
    List<OrderSystem.Result> resultList = new ArrayList<>();
    for (Tuple tuple : tupleList)
      resultList.add(new ResultImpl(tuple, keys));
    return resultList.iterator();
  }

  public OrderSystem.KeyValue sumOrdersByGood(
      String goodid, String key) throws Exception {

    List<String> keys = Collections.singletonList(key);
    List<Tuple> tupleList = good2OrderHashTable.getMulti(goodid.getBytes(), null);
    for (Tuple tuple : tupleList) {
      OrderSystem.KeyValue kv = new ResultImpl(tuple, keys).get(key);
      kv.valueAsLong();
      kv.valueAsDouble();

    }
    return null;
  }

//  private void readOrderFile(String filename, int fileId) throws Exception {
//    BufferedInputStream bis = new BufferedInputStream(new FileInputStream(filename));
//    int b, keyLen = 0, valueLen = 0;
//    long offset = 0, count = 0;
//    // 0 for read key, 1 for read value, 2 for skip line
//    int status = 0;
//    byte[] key = new byte[256];
//    byte[] value = new byte[65536];
//    while ((b = bis.read()) != -1) {
//      count++;
//      if (status == 0) {
//        if (b == ':') {
//          valueLen = 0;
//          status = 1;
//        } else {
//          key[keyLen++] = (byte) b;
//        }
//      } else if (status == 1) {
//        if (b == '\t') {
//          //print(key, keyLen, value, valueLen);
//
//          if (expectedKey(key, keyLen, orderidBytes)) {
//            long valueLong = Long.parseLong(new String(value, 0, valueLen));
//            orderHashTable.add(valueLong, fileId, offset);
//            //orderHashTable.get(Util.long2byte(valueLong));
//            status = 2;
//          } else {
//            keyLen = 0;
//            status = 0;
//          }
//        } else if (b == '\n') {
//          //print(key, keyLen, value, valueLen);
//          //System.out.println("offset: " + offset);
//
//          if (expectedKey(key, keyLen, orderidBytes)) {
//            long valueLong = Long.decode(new String(value, 0, valueLen));
//            orderHashTable.add(valueLong, fileId, offset);
//            //orderHashTable.get(Util.long2byte(valueLong));
//          }
//
//          offset = count;
//          keyLen = 0;
//          status = 0;
//        } else {
//          value[valueLen++] = (byte) b;
//        }
//      } else {
//        if (b == '\n') {
//          //System.out.println("offset: " + offset);
//
//          offset = count;
//          keyLen = 0;
//          status = 0;
//        }
//      }
//    }
//  }

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
