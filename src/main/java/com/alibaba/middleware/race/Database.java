package com.alibaba.middleware.race;

import com.alibaba.middleware.race.index.BgIndex;
import com.alibaba.middleware.race.index.OrderIndex;
import com.alibaba.middleware.race.kvDealer.*;
import com.alibaba.middleware.race.result.BuyerResult;
import com.alibaba.middleware.race.result.GoodResult;
import com.alibaba.middleware.race.result.OrderResult;
import com.alibaba.middleware.race.result.SimpleResult;

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 * Created by yfy on 7/13/16.
 * Database
 */
public class Database {

  private List<String> orderFilesList, goodFilesList, buyerFilesList,
      storeFoldersList;

  private static BuyerResultComparator buyerResultComparator;

  private static GoodResultComparator goodResultComparator;

  private OrderIndex orderIndex;

  public static BgIndex buyerIndex, goodIndex;

  public Database(Collection<String> orderFiles,
                  Collection<String> buyerFiles,
                  Collection<String> goodFiles,
                  Collection<String> storeFolders) throws Exception {

    buyerResultComparator = new BuyerResultComparator();
    goodResultComparator = new GoodResultComparator();

    orderFilesList = new ArrayList<>();
    for (String file : orderFiles)
      orderFilesList.add(file);

    goodFilesList = new ArrayList<>();
    for (String file : goodFiles)
      goodFilesList.add(file);

    buyerFilesList = new ArrayList<>();
    for (String file : buyerFiles)
      buyerFilesList.add(file);

    storeFoldersList = new ArrayList<>();
    for (String folder : storeFolders)
      storeFoldersList.add(folder);
  }

  public void construct() throws Exception {
    buildO2oHash();
    buildG2oHash();
    buildB2oHash();
    buildG2gHash();
    buildB2bHash();
    loadO2o1DirectMemory();
    FdMap.init(orderFilesList, goodFilesList, buyerFilesList,
        fullname2("b2o.dat"), fullname1("g2o.dat"));
  }

  private void loadO2o1DirectMemory() {
    try {
      FileInputStream fis = new FileInputStream(fullname1("o2o.idx"));
      FileChannel channel = fis.getChannel();
      int buffer1Size = Config.orderIndexBuffer1BlockNum * 4096;
      int buffer2Size = (int) (channel.size() - buffer1Size);
      ByteBuffer buffer1 = ByteBuffer.allocateDirect(buffer1Size);
      ByteBuffer buffer2 = ByteBuffer.allocateDirect(buffer2Size);
      channel.read(buffer1);
      channel.read(buffer2);
      orderIndex.setTable1DirectMemory(buffer1, buffer2);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void buildO2oHash() throws Exception {
    System.out.println(System.currentTimeMillis() + " [yfy] buildO2o");
    orderIndex = new OrderIndex(orderFilesList);
    O2oKvDealer dealer = new O2oKvDealer(orderIndex);

    int mid = orderFilesList.size() / 2;

    orderIndex.setCurrentTable(0, fullname0("o2o.idx"));
    for (int i = 0; i < mid; i++) {
      dealer.setFileId(i);
      readDataFile(orderFilesList.get(i), dealer);
    }
    orderIndex.finish();
    System.gc();

    orderIndex.setCurrentTable(1, fullname1("o2o.idx"));
    for (int i = mid; i < orderFilesList.size(); i++) {
      dealer.setFileId(i);
      readDataFile(orderFilesList.get(i), dealer);
    }
    orderIndex.finish();
    System.gc();
  }

  private void buildG2oHash() throws Exception {
    System.out.println(System.currentTimeMillis() + " [yfy] buildG2o");
    goodIndex = new BgIndex(orderFilesList, goodFilesList,
        Config.goodIndexSize, Config.goodIndexBlockSize,
        Config.g2gIndexSize, Config.bg2bgIndexBlockSize);
    G2oKvDealer dealer = new G2oKvDealer(goodIndex);
    goodIndex.setCurrentTable(0, fullname1("g2o.idx"));
    for (int i = 0; i < orderFilesList.size(); i++) {
      dealer.setFileId(i);
      readDataFile(orderFilesList.get(i), dealer);
    }
    goodIndex.finish();
    System.gc();
  }

  private void buildB2oHash() throws Exception {
    System.out.println(System.currentTimeMillis() + " [yfy] buildB2o");
    buyerIndex = new BgIndex(orderFilesList, buyerFilesList,
        Config.buyerIndexSize, Config.buyerIndexBlockSize,
        Config.b2bIndexSize, Config.bg2bgIndexBlockSize);
    B2oKvDealer dealer = new B2oKvDealer(buyerIndex);
    buyerIndex.setCurrentTable(0, fullname2("b2o.idx"));
    for (int i = 0; i < orderFilesList.size(); i++) {
      dealer.setFileId(i);
      readDataFile(orderFilesList.get(i), dealer);
    }
    buyerIndex.finish();
    System.gc();
  }



  private void buildB2bHash() throws Exception {
    BuyerKvDealer dealer = new BuyerKvDealer(buyerIndex);
    for (int i = 0; i < buyerFilesList.size(); i++) {
      dealer.setFileId(i);
      readDataFile(buyerFilesList.get(i), dealer);
    }
  }

  private void buildG2gHash() throws Exception {
    GoodKvDealer dealer = new GoodKvDealer(goodIndex);
    for (int i = 0; i < goodFilesList.size(); i++) {
      dealer.setFileId(i);
      readDataFile(goodFilesList.get(i), dealer);
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

    System.out.println("[yfy] filename: " + filename +
        " size: " + new File(filename).length());
    ReadBuffer readBuffer = new ReadBuffer(filename);
    new Thread(readBuffer).start();
    readBuffer.getBuf();

    int b, keyLen = 0, valueLen = 0;
    long offset = 0, count = 0;
    // 0 for read key, 1 for read value, 2 for skip line
    int status = 0;
    byte[] key = new byte[256];
    byte[] value = new byte[100000];

    while ((b = readBuffer.read()) != -1) {
      //System.out.print((char)b);
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

  public OrderResult queryOrder(long orderId, Collection<String> keys)
      throws Exception {

    if (orderId > Config.orderidMax || orderId < Config.orderidMin)
      return null;
    Tuple orderTuple = orderIndex.get(Util.long2byte5(orderId));
    if (orderTuple == null)
      return null;
    return new OrderResult(orderTuple, keys);
  }

  public Iterator<OrderSystem.Result> queryOrdersByBuyer(
      long startTime, long endTime, String buyerid) throws Exception {

    List<Tuple> orderTupleList = buyerIndex.getOrder(buyerid, true);
    if (orderTupleList.isEmpty())
      return new ArrayList<OrderSystem.Result>().iterator();

    Tuple buyerTuple = buyerIndex.getBg(buyerid);
    SimpleResult buyerResult = new SimpleResult(buyerTuple, null);

    List<BuyerResult> resultListAll = new ArrayList<>(orderTupleList.size());
    for (Tuple tuple : orderTupleList)
      resultListAll.add(new BuyerResult(tuple, buyerResult));
    if (resultListAll.get(0).orderTuple.isRecord()) { // savedat
      Collections.sort(resultListAll, buyerResultComparator);
      buyerIndex.saveBuyerAll(resultListAll, buyerid);
    }

    List<OrderSystem.Result> resultList = new ArrayList<>();
    for (BuyerResult br : resultListAll) {
      long time = br.getCreatetime();
      if (time >= startTime && time <= endTime)
        resultList.add(br);
    }

    return resultList.iterator();
  }

  public Iterator<OrderSystem.Result> queryOrdersBySaler(
      String goodid, Collection<String> keys) throws Exception {

    List<Tuple> tupleList = goodIndex.getOrder(goodid, false);
    if (tupleList.isEmpty())
      return new ArrayList<OrderSystem.Result>().iterator();

    Tuple goodTuple = goodIndex.getBg(goodid);
    SimpleResult goodResult = new SimpleResult(goodTuple, keys);

    List<GoodResult> resultList = new ArrayList<>(tupleList.size());
    for (Tuple tuple : tupleList)
      resultList.add(new GoodResult(tuple, goodResult, keys));
    for (GoodResult result : resultList)
      result.phase2();
    if (resultList.get(0).orderTuple.isRecord()) {
      Collections.sort(resultList, goodResultComparator);
      goodIndex.saveGoodAll(resultList, goodid);
    }

    List<OrderSystem.Result> returnList = new ArrayList<>(resultList.size());
    for (GoodResult result : resultList)
      returnList.add(result);
    return returnList.iterator();
  }

  public OrderSystem.KeyValue sumOrdersByGood(
      String goodid, String key) throws Exception {

    boolean asLong = true, asDouble = true, hasKey = false;
    long sumLong = 0;
    double sumDouble = 0;

    Collection<String> keys = Collections.singleton(key);
    List<Tuple> orderTupleList = goodIndex.getOrder(goodid, false);
    if (orderTupleList.isEmpty())
      return null;

    Tuple goodTuple = goodIndex.getBg(goodid);
    SimpleResult simpleGoodResult = new SimpleResult(goodTuple, keys);
    OrderSystem.KeyValue kv = simpleGoodResult.get(key);
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

    List<GoodResult> goodResultList = new ArrayList<>(orderTupleList.size());
    for (Tuple tuple : orderTupleList) {
      long valueLong = 0;
      double valueDouble = 0;

      GoodResult goodResult = new GoodResult(tuple, simpleGoodResult, keys);
      goodResult.phase2();
      goodResultList.add(goodResult);
      kv = goodResult.get(key);

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

    if (goodResultList.get(0).orderTuple.isRecord()) {
      Collections.sort(goodResultList, goodResultComparator);
      goodIndex.saveGoodAll(goodResultList, goodid);
    }

    if (!hasKey) return null;

    return new KeyValueForSum(key, sumLong, sumDouble);
  }

  //  private void buildBg2oHash() throws Exception {
//    System.out.println(System.currentTimeMillis() + " [yfy] buildBg2o");
//    buyerIndex = new BgIndex(orderFilesList, buyerFilesList,
//        Config.buyerIndexSize, Config.buyerIndexBlockSize,
//        Config.b2bIndexSize, Config.bg2bgIndexBlockSize);
//    goodIndex = new BgIndex(orderFilesList, goodFilesList,
//        Config.goodIndexSize, Config.goodIndexBlockSize,
//        Config.g2gIndexSize, Config.bg2bgIndexBlockSize);
//    Bg2oKvDealer dealer = new Bg2oKvDealer(buyerIndex, goodIndex);
//
//    int mid = orderFilesList.size() / 2;
//
//    buyerIndex.setCurrentTable(0, fullname1("b2o.idx"));
//    goodIndex.setCurrentTable(0, fullname2("g2o.idx"));
//    for (int i = 0; i < mid; i++) {
//      dealer.setFileId(i);
//      readDataFile(orderFilesList.get(i), dealer);
//    }
//    buyerIndex.finish();
//    goodIndex.finish();
//    System.gc();
//
//    buyerIndex.setCurrentTable(1, fullname2("b2o.idx"));
//    goodIndex.setCurrentTable(1, fullname0("g2o.idx"));
//    for (int i = mid; i < orderFilesList.size(); i++) {
//      dealer.setFileId(i);
//      readDataFile(orderFilesList.get(i), dealer);
//    }
//    buyerIndex.finish();
//    goodIndex.finish();
//    System.gc();
//  }

  //  private void buildO2oHash() throws Exception {
//    System.out.println(System.currentTimeMillis() + " [yfy] buildO2o");
//    orderIndex = new OrderIndex(orderFilesList);
//    O2oKvDealer dealer = new O2oKvDealer(orderIndex);
//    for (int diskId = 0; diskId < 3; diskId++) {
//      String indexFile = storeFoldersList.get(diskId) + "/o2o.idx";
//      orderIndex.setCurrentTable(diskId, indexFile);
//      for (int i = 0; i < orderFilesList.size(); i++) {
//        String filename = orderFilesList.get(i);
//        dealer.setFileId(i);
//        if (filename.charAt(5) == '1' + diskId)
//          readDataFile(filename, dealer);
//      }
//      orderIndex.finish();
//      System.gc();
//    }
//
////    System.out.println("[yfy] order num: " + OrderKvDealer.count);
////    System.out.println("[yfy] orderid max: " + OrderKvDealer.maxOid +
////        " min: " + OrderKvDealer.minOid);
////    System.out.println("[yfy] buyer max len " + OrderKvDealer.maxBl +
////        " min len " + OrderKvDealer.minBl);
////    System.out.println("[yfy] good max len " + OrderKvDealer.maxGl +
////        " min len " + OrderKvDealer.minGl);
//  }

  //  private void buildB2oHash() throws Exception {
//    System.out.println(System.currentTimeMillis() + " [yfy] buildB2o");
//    buyerIndex = new BgIndex(orderFilesList,
//        storeFoldersList.get(1) + "/b2o.idx", buyerFilesList,
//        Config.buyerIndexSize, Config.buyerIndexBlockSize);
//    buyerIndex.setBgTable(37735, 8192); // load factor 0.75
//    OrderKvDealer dealer = new OrderKvDealer(null, buyerIndex, null);
//    for (int i = 0; i < orderFilesList.size(); i++) {
//      dealer.setFileId(i);
//      readDataFile(orderFilesList.get(i), dealer);
//    }
//    buyerIndex.finish();
//  }
//
//  private void buildG2oHash() throws Exception {
//    System.out.println(System.currentTimeMillis() + " [yfy] buildG2o");
//    goodIndex = new BgIndex(orderFilesList,
//        storeFoldersList.get(2) + "/g2o.idx", goodFilesList,
//        Config.goodIndexSize, Config.goodIndexBlockSize);
//    goodIndex.setBgTable(18867, 8192); // load factor 0.75
//    OrderKvDealer dealer = new OrderKvDealer(null, null, goodIndex);
//    for (int i = 0; i < orderFilesList.size(); i++) {
//      dealer.setFileId(i);
//      readDataFile(orderFilesList.get(i), dealer);
//    }
//    goodIndex.finish();
//  }

//  private void buildOrder2OrderHash() throws Exception {
//    orderIndex = new OrderIndex(orderFilesList);
//    buyerIndex = new BgIndex(orderFilesList, fullname1("b2o.idx"),
//        buyerFilesList, Config.buyerIndexSize, Config.buyerIndexBlockSize);
//    goodIndex = new BgIndex(orderFilesList, fullname2("g2o.idx"),
//        goodFilesList, Config.goodIndexSize, Config.goodIndexBlockSize);
//
//    System.out.println(System.currentTimeMillis());
//    OrderKvDealer dealer = new OrderKvDealer(orderIndex, buyerIndex, goodIndex);
//    for (int i = 0; i < orderFilesList.size(); i++) {
//      dealer.setFileId(i);
//      readDataFile(orderFilesList.get(i), dealer);
//    }
//    System.out.println(System.currentTimeMillis());
//    System.out.println("[yfy] order num: " + OrderKvDealer.count);
//    System.out.println("[yfy] orderid max: " + OrderKvDealer.maxOid +
//        " min: " + OrderKvDealer.minOid);
//    System.out.println("[yfy] buyer max orderNum " + buyerIndex.maxOrderNum());
//    System.out.println("[yfy] good max orderNum " + goodIndex.maxOrderNum());
//  }

//  private void readAio(String filename) throws Exception {
//    final int SIZE = 50;
//    Path path = Paths.get(filename);
//    final AsynchronousFileChannel channel = AsynchronousFileChannel.open(path);
//    final ByteBuffer buffer0 = ByteBuffer.allocate(SIZE);
//    final ByteBuffer buffer1 = ByteBuffer.allocate(SIZE);
//    CompletionHandler<Integer, Object> handler =
//        new CompletionHandler<Integer, Object>() {
//          public int offset;
//
//          @Override
//          public void completed(Integer result, Object att) {
//            System.out.println(new String(buffer0.array()));
//            offset += SIZE;
//            System.out.println(offset);
//            buffer0.clear();
//            channel.read(buffer0, offset, null, this);
//          }
//
//          @Override
//          public void failed(Throwable exc, Object att) {
//            System.out.println("fail");
//          }
//        };
//    channel.read(buffer0, 0, null, handler);
//
//    System.out.println("other");
//    Thread.sleep(3000);
//  }

  //  private void print(byte[] key, int keyLen, byte[] value, int valueLen) {
//    for (int i = 0; i < keyLen; i++)
//      System.out.print((char) key[i]);
//    System.out.print(':');
//    for (int i = 0; i < valueLen; i++)
//      System.out.print((char) value[i]);
//    System.out.print('\t');
//  }

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
