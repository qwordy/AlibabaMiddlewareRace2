package com.alibaba.middleware.race;

import com.alibaba.middleware.race.cache.ConcurrentCache;
import com.alibaba.middleware.race.index.BgIndex;
import com.alibaba.middleware.race.index.OrderIndex;
import com.alibaba.middleware.race.kvDealer.BuyerKvDealer;
import com.alibaba.middleware.race.kvDealer.GoodKvDealer;
import com.alibaba.middleware.race.kvDealer.IKvDealer;
import com.alibaba.middleware.race.kvDealer.OrderKvDealer;
import com.alibaba.middleware.race.result.BuyerResult;
import com.alibaba.middleware.race.result.GoodResult;
import com.alibaba.middleware.race.result.SimpleResult;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.util.*;

/**
 * Created by yfy on 7/13/16.
 * Database
 */
public class Database {

  private List<String> orderFilesList, goodFilesList, buyerFilesList,
      storeFoldersList;

  private static TupleCreatetimeComparator tupleCreatetimeComparator;

  private static TupleOrderidComparator tupleOrderidComparator;

  private OrderIndex orderIndex;

  public static BgIndex buyerIndex, goodIndex;

  private Thread writeBuffer0Thread, writeBuffer1Thread,
      writeBuffer2Thread;

  public Database(Collection<String> orderFiles,
                  Collection<String> buyerFiles,
                  Collection<String> goodFiles,
                  Collection<String> storeFolders) throws Exception {

    tupleCreatetimeComparator = new TupleCreatetimeComparator();
    tupleOrderidComparator = new TupleOrderidComparator();

    ConcurrentCache cache = ConcurrentCache.getInstance();

    orderFilesList = new ArrayList<>();
    for (String file : orderFiles) {
      orderFilesList.add(file);
      cache.addFd(file, true);
    }

    goodFilesList = new ArrayList<>();
    for (String file : goodFiles) {
      goodFilesList.add(file);
      cache.addFd(file, true);
    }

    buyerFilesList = new ArrayList<>();
    for (String file : buyerFiles) {
      buyerFilesList.add(file);
      cache.addFd(file, true);
    }

    storeFoldersList = new ArrayList<>();
    for (String folder : storeFolders)
      storeFoldersList.add(folder);
  }

  public void construct() throws Exception {
    buildOrder2OrderHash();
    buildGood2GoodHash();
    buildBuyer2BuyerHash();
    writeBuffer0Thread.join();
    writeBuffer1Thread.join();
    writeBuffer2Thread.join();
  }

  private void buildOrder2OrderHash() throws Exception {
    WriteBuffer writeBuffer0 = new WriteBuffer(1 << 21, 200, 4096);
    WriteBuffer writeBuffer1 = new WriteBuffer(85, 24, 1024);
    WriteBuffer writeBuffer2 = new WriteBuffer(45, 24, 2048);
    writeBuffer0Thread = new Thread(writeBuffer0);
    writeBuffer1Thread = new Thread(writeBuffer1);
    writeBuffer2Thread = new Thread(writeBuffer2);
    orderIndex = new OrderIndex(orderFilesList, fullname0("order.idx"),
        writeBuffer0);
    buyerIndex = new BgIndex(orderFilesList, fullname1("b2o.idx"),
        buyerFilesList, 8500000, 1024, writeBuffer1);
    goodIndex = new BgIndex(orderFilesList, fullname2("g2o.idx"),
        goodFilesList, 4500000, 2048, writeBuffer2);
    writeBuffer0Thread.start();
    //writeBuffer1Thread.start();
    //writeBuffer2Thread.start();

    OrderKvDealer dealer = new OrderKvDealer(orderIndex, buyerIndex, goodIndex);
    for (int i = 0; i < orderFilesList.size(); i++) {
      dealer.setFileId(i);
      readDataFile(orderFilesList.get(i), dealer);
    }

    writeBuffer0.finish();
    writeBuffer1.finish();
    writeBuffer2.finish();

  }

  private void buildGood2GoodHash() throws Exception {
    GoodKvDealer dealer = new GoodKvDealer(goodIndex);
    for (int i = 0; i < goodFilesList.size(); i++) {
      dealer.setFileId(i);
      readDataFile(goodFilesList.get(i), dealer);
    }
  }

  private void buildBuyer2BuyerHash() throws Exception {
    BuyerKvDealer dealer = new BuyerKvDealer(buyerIndex);
    for (int i = 0; i < buyerFilesList.size(); i++) {
      dealer.setFileId(i);
      readDataFile(buyerFilesList.get(i), dealer);
    }
  }

  private String fullname0(String filename) {
    return storeFoldersList.get(0) + '/' + filename;
  }

  private String fullname1(String filename) {
    return storeFoldersList.get(1) + '/' + filename;
  }

  private String fullname2(String filename) {
    return storeFoldersList.get(2) + '/' + filename;
  }

  private void readDataFile(String filename, IKvDealer dealer)
      throws Exception {

    BufferedInputStream bis =
        new BufferedInputStream(new FileInputStream(filename));

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

//  private void print(byte[] key, int keyLen, byte[] value, int valueLen) {
//    for (int i = 0; i < keyLen; i++)
//      System.out.print((char) key[i]);
//    System.out.print(':');
//    for (int i = 0; i < valueLen; i++)
//      System.out.print((char) value[i]);
//    System.out.print('\t');
//  }

  public ResultImpl queryOrder(long orderId, Collection<String> keys)
      throws Exception {

    Tuple orderTuple = orderIndex.get(Util.long2byte(orderId));
    if (orderTuple == null)
      return null;
    ResultImpl result = new ResultImpl(orderTuple, keys);
    //result.printOrderTuple();
    return result;
  }

  public Iterator<OrderSystem.Result> queryOrdersByBuyer(
      long startTime, long endTime, String buyerid) throws Exception {

    //TupleFilter filter = new TupleFilter(startTime, endTime);
    List<Tuple> orderTupleList = buyerIndex.getOrder(buyerid); // filter
    if (orderTupleList.isEmpty())
      return new ArrayList<OrderSystem.Result>().iterator();

    Tuple buyerTuple = buyerIndex.getBg(buyerid);
    SimpleResult buyerResult = new SimpleResult(buyerTuple, null);

    Collections.sort(orderTupleList, tupleCreatetimeComparator);
    List<OrderSystem.Result> resultList = new ArrayList<>();
    for (Tuple tuple : orderTupleList) {
      if (tuple.getData() >= startTime && tuple.getData() <= endTime)
        resultList.add(new BuyerResult(tuple, buyerResult));
    }
    return resultList.iterator();
  }

  public Iterator<OrderSystem.Result> queryOrdersBySaler(
      String goodid, Collection<String> keys) throws Exception {

    List<Tuple> tupleList = goodIndex.getOrder(goodid);
    if (tupleList.isEmpty())
      return new ArrayList<OrderSystem.Result>().iterator();

    Tuple goodTuple = goodIndex.getBg(goodid);
    SimpleResult goodResult = new SimpleResult(goodTuple, keys);

    Collections.sort(tupleList, tupleOrderidComparator);
    List<OrderSystem.Result> resultList = new ArrayList<>();
    for (Tuple tuple : tupleList)
      resultList.add(new GoodResult(tuple, goodResult, keys));
    return resultList.iterator();
  }

  public OrderSystem.KeyValue sumOrdersByGood(
      String goodid, String key) throws Exception {

    boolean asLong = true, asDouble = true, hasKey = false;
    long sumLong = 0;
    double sumDouble = 0;

    Collection<String> keys = Collections.singleton(key);
    List<Tuple> orderTupleList = goodIndex.getOrder(goodid);

    Tuple goodTuple = goodIndex.getBg(goodid);
    SimpleResult goodResult = new SimpleResult(goodTuple, keys);
    OrderSystem.KeyValue kv = goodResult.get(key);
    if (kv != null) {
      long vl = 0;
      double vd = 0;
      try {
        vl = kv.valueAsLong();
      } catch (Exception e) {
        asLong = false;
      }
      try {
        vd = kv.valueAsDouble();
      } catch (Exception e) {
        asDouble = false;
      }
      if (!asLong && !asDouble)
        return null;
      int size = orderTupleList.size();
      return new KeyValueForSum(key, vl * size, vd * size);
    }

    for (Tuple tuple : orderTupleList) {
      long valueLong = 0;
      double valueDouble = 0;
      kv = new ResultImpl(tuple, keys).get(key);

      if (kv == null)
        continue;
      else
        hasKey = true;

      if (asLong) {
        try {
          valueLong = kv.valueAsLong();
          sumLong += valueLong;
        } catch (OrderSystem.TypeException e) {
          asLong = false;
        }
      }

      if (asDouble) {
        try {
          valueDouble = kv.valueAsDouble();
          sumDouble += valueDouble;
        } catch (OrderSystem.TypeException e) {
          asDouble = false;
        }
      }

      if (!asLong && !asDouble)
        return null;
    }

    if (!hasKey)
      return null;

    return new KeyValueForSum(key, sumLong, sumDouble);
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
