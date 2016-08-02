package com.alibaba.middleware.race.index;

import com.alibaba.middleware.race.*;
import com.alibaba.middleware.race.result.BuyerResult;
import com.alibaba.middleware.race.result.GoodResult;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yfy on 7/24/16.
 * Buyer or good index
 */
public class BgIndex {

  //private HashTable[] orderTables;
  private HashTable orderTable;

  private int orderTableId;

  // bg 2 bg
  private HashTable bgTable;

  private List<String> orderFiles;

  private int size, blockSize, count;

  private BgBytes bgBytes;

  public BgIndex(List<String> orderFiles, List<String> bgFiles,
                 int size, int blockSize,
                 int bgSize, int bgBlockSize) {

    this.orderFiles = orderFiles;
    this.size = size;
    this.blockSize = blockSize;
    bgBytes = new BgBytes();
    //orderTables = new HashTable[2];
    bgTable = new HashTable(bgFiles, null, bgSize, bgBlockSize, 29);
  }

  // 0 or 1
  public void setCurrentTable(int id, String indexFile) {
    //orderTableId = id;
    //orderTables[id] = new HashTable(orderFiles, indexFile, size, blockSize, 5);
    orderTable = new HashTable(orderFiles, indexFile, size, blockSize, 5);
  }

  public void finish() throws Exception {
    //orderTables[orderTableId].writeFile();
    orderTable.writeFile();
    bgTable.printBgIndexSize();
  }

  public void addOrder(byte[] bg, int len, int fildId, long fildOff) {
    int bgNo;
    boolean find = bgTable.getBg(bg, len, bgBytes, true);
    if (find) {
      bgNo = Util.byte3Toint(bgBytes.block, bgBytes.off + 5);
    } else {
      bgNo = count++;
      Util.int2byte3(bgNo, bgBytes.block, bgBytes.off + 5);
    }
    //orderTables[orderTableId].add(null, bgNo, fildId, fildOff);
    orderTable.add(null, bgNo, fildId, fildOff);
  }

  // add all order then add bg
  public void addBg(byte[] bg, int len, int fileId, long fileOff) {
    boolean find = bgTable.getBg(bg, len, bgBytes, true);
    bgBytes.block[bgBytes.off] = (byte) fileId;
    Util.longToByte4(fileOff, bgBytes.block, bgBytes.off + 1);
    if (!find)
      Util.int2byte3(0xffffff, bgBytes.block, bgBytes.off + 5);
  }

  public List<Tuple> getOrder(String bg) throws Exception {
    int len = bg.length();
    if (len != 20 && len != 21)
      return new ArrayList<>();
    Integer bgId = bgTable.getBgId(bg.getBytes(), len);
    if (bgId == null || bgId == 0xffffff)
      return new ArrayList<>();
//    List<Tuple> list0 = orderTables[0].getAll(bgId);
//    List<Tuple> list1 = orderTables[1].getAll(bgId);
//    for (Tuple tuple : list1)
//      list0.add(tuple);
//    return list0;
    return orderTable.getAll(bgId);
  }

  public List<Tuple> getBuyerOrder(String bg) throws Exception {
    int len = bg.length();
    if (len != 20 && len != 21)
      return new ArrayList<>();

    BgBytes bgBytes = new BgBytes();
    boolean find = bgTable.getBg(bg.getBytes(), len, bgBytes, false);
    if (!find) return new ArrayList<>();

    int bgNo = Util.byte3Toint(bgBytes.block, bgBytes.off + 5);
    if (bgNo == 0xffffff) return new ArrayList<>();
    if (bgNo == 0xfffff0) { // already record
      long off = Util.byte5ToLong(bgBytes.block, bgBytes.off); // off in b2odat
      byte[] buf = new byte[8];
      RandomAccessFile fd = FdMap.b2oDat;
      String g2oDatFilename = FdMap.b2oDatFilename;
      List<Tuple> tupleList;
      synchronized (fd) {
        fd.seek(off);
        fd.read(buf, 0, 4);
        int count = Util.byte2int(buf, 0);
        tupleList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
          fd.read(buf, 0, 8);
          long tupleOff = Util.byte2long(buf, 0);
          tupleList.add(new Tuple(g2oDatFilename, tupleOff));
        }
      }
      return tupleList;
    } else {
      List<Tuple> list = orderTable.getAll(bgNo);
      for (Tuple tuple : list)
        tuple.setRecord();
      return list;
    }
  }

  public void saveBuyerOrder(List<BuyerResult> resultList, String bg)
      throws Exception {

    BgBytes bgBytes = new BgBytes();
    boolean find = bgTable.getBg(bg.getBytes(), bg.length(), bgBytes, false);
    if (!find) return;

    int bgNo = Util.byte3Toint(bgBytes.block, bgBytes.off + 5);
    if (bgNo == 0xfffff0) return; // already in

    int size = resultList.size();
    RandomAccessFile fd = FdMap.b2oDat;
    byte[] buf = new byte[8];
    synchronized (fd) {
      long fileLen = fd.length();
      Util.int2byte(size, buf, 0);
      fd.seek(fileLen);
      fd.write(buf, 0, 4);  // size
      long tupleOff = fileLen + 4 + 8 * size;
      // write head: size, off, off...
      for (BuyerResult result : resultList) {
        Util.long2byte(tupleOff, buf, 0);
        fd.write(buf, 0, 8);  // off
        Tuple orderTuple = result.getOrderTuple();
        Tuple goodTuple = result.getGoodTuple();
        if (goodTuple == null)
          throw new Exception();
        tupleOff += orderTuple.getTupleLen() + goodTuple.getTupleLen() + 2;
      }
      for (BuyerResult result : resultList) {
        Tuple orderTuple = result.getOrderTuple();
        Tuple goodTuple = result.getGoodTuple();

        List<byte[]> tupleContent = orderTuple.getTupleContent();
        int tupleLen = orderTuple.getTupleLen();
        int startOff = orderTuple.getTupleStartOff();
        int blockNum = tupleContent.size();
        if (blockNum == 1) {
          fd.write(tupleContent.get(0), startOff, tupleLen);
        } else {
          fd.write(tupleContent.get(0), startOff, 4096 - startOff);
          for (int i = 1; i < blockNum; i++)
            fd.write(tupleContent.get(i));
        }
        fd.write('\t');

        tupleContent = goodTuple.getTupleContent();
        tupleLen = goodTuple.getTupleLen();
        startOff = goodTuple.getTupleStartOff();
        blockNum = tupleContent.size();
        if (blockNum == 1) {
          fd.write(tupleContent.get(0), startOff, tupleLen);
        } else {
          fd.write(tupleContent.get(0), startOff, 4096 - startOff);
          for (int i = 1; i < blockNum; i++)
            fd.write(tupleContent.get(i));
        }
        fd.write('\n');
      }
    }
    // set bgId a special value
    Util.int2byte3(0xfffff0, bgBytes.block, bgBytes.off + 5);
  }

  public List<Tuple> getGoodOrder(String goodid) throws Exception {
    int len = goodid.length();
    if (len != 20 && len != 21)
      return new ArrayList<>();

    BgBytes goodBytes = new BgBytes();
    boolean find = bgTable.getBg(goodid.getBytes(), len, goodBytes, false);
    if (!find)
      return new ArrayList<>();

    int bgNo = Util.byte3Toint(bgBytes.block, bgBytes.off + 5);
    if (bgNo == 0xffffff)
      return new ArrayList<>();

    if (bgNo == 0xfffff0) { // tuples have been stored in g2o.dat
      long off = Util.byte5ToLong(bgBytes.block, bgBytes.off);
      byte[] buf = new byte[8];
      RandomAccessFile fd = FdMap.g2oDat;
      String g2oDatFilename = FdMap.g2oDatFilename;
      fd.seek(off);
      fd.read(buf, 0, 4);
      int count = Util.byte2int(buf, 0);
      List<Tuple> tupleList = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        fd.read(buf, 0, 8);
        long tupleOff = Util.byte2long(buf, 0);
        tupleList.add(new Tuple(g2oDatFilename, tupleOff));
      }
      return tupleList;
    } else {
      return orderTable.getAll(bgNo);
    }
  }

  public void saveOrderTuples(List<GoodResult> resultList, String goodid)
      throws Exception {

    BgBytes goodBytes = new BgBytes();
    boolean find = bgTable.getBg(goodid.getBytes(), goodid.length(),
        goodBytes, false);
    if (!find) return;

    int bgNo = Util.byte3Toint(bgBytes.block, bgBytes.off + 5);
    if (bgNo == 0xfffff0)  // already in
      return;

    int size = resultList.size();
    RandomAccessFile fd = FdMap.g2oDat;
    byte[] buf = new byte[8];
    synchronized (fd) {
      long fileLen = fd.length();
      Util.int2byte(size, buf, 0);
      fd.seek(fileLen);
      fd.write(buf, 0, 4);
      long tupleOff = fileLen + 4 + 8 * size;
      // write head: size, off, off...
      for (GoodResult result : resultList) {
        Util.long2byte(tupleOff, buf, 0);
        fd.write(buf, 0, 8);

        Tuple orderTuple = result.getOrderTuple();
        Tuple buyerTuple = result.getBuyerTuple();
        int tuplesLen = orderTuple.getTupleLen() + 1;
        if (buyerTuple.isRecord()) {
          tuplesLen += buyerTuple.getTupleLen() + 1;
        }
        tupleOff = tuplesLen;
      }
      // write tuples
      for (GoodResult result : resultList) {
        Tuple orderTuple = result.getOrderTuple();
        Tuple buyerTuple = result.getBuyerTuple();

        List<byte[]> tupleContent = orderTuple.getTupleContent();
        int tupleLen = orderTuple.getTupleLen();
        int startOff = orderTuple.getTupleStartOff();
        int blockNum = tupleContent.size();
        if (blockNum == 1) {
          fd.write(tupleContent.get(0), startOff, tupleLen);
        } else {
          fd.write(tupleContent.get(0), startOff, 4096 - startOff);
          for (int i = 1; i < blockNum; i++)
            fd.write(tupleContent.get(i));
        }

        if (buyerTuple.isRecord()) {
          fd.write('\t');
          tupleContent = buyerTuple.getTupleContent();
          tupleLen = buyerTuple.getTupleLen();
          startOff = buyerTuple.getTupleStartOff();
          blockNum = tupleContent.size();
          if (blockNum == 1) {
            fd.write(tupleContent.get(0), startOff, tupleLen);
          } else {
            fd.write(tupleContent.get(0), startOff, 4096 - startOff);
            for (int i = 1; i < blockNum; i++)
              fd.write(tupleContent.get(i));
          }
        }
        fd.write('\n');
      }
    }
  }

  public Tuple getBg(String bg) {
    int len = bg.length();
    if (len != 20 && len != 21)
      return null;
    return bgTable.getBgTuple(bg.getBytes(), len);
  }

//  public void printInfo(String tag) {
//    int[] nums = new int[12];
//    int max = 0;
//    for (Value value : map.values()) {
//      nums[value.orderNum / 50]++;
//      if (value.orderNum > max)
//        max = value.orderNum;
//    }
//
//    System.out.println("[yfy] " + tag + " max order num: " + max);
//    System.out.print("[yfy] dist: ");
//    for (int i = 0; i < 12; i++)
//      System.out.print(nums[i] + " ");
//    System.out.println();
//  }

//  private static class Value {
//    short fileId;  // bg file id
//    long fileOff;  // bg file off
//    int orderNum;
//    int blockNo;
//
//    Value(short fileId, long fileOff, int blockNo) {
//      this.fileId = fileId;
//      this.fileOff = fileOff;
//      this.blockNo = blockNo;
//      orderNum = 1;
//    }
//  }

  //System.out.println("bg addOrder");
//    String buyerStr = new String(bg, 0, len);
//    Value value = map.get(buyerStr);
//    int blockNo;
//    if (value == null) {
//      blockNo = count;
//      map.put(buyerStr, new Value((short) 0, 0, count));
//      count++;
//    } else {
//      blockNo = value.blockNo;
//      value.orderNum++;
//    }
//    //table.add(null, blockNo, fildId, fildOff);

  //  public void addBg(byte[] bg, int len, int fileId, long fileOff) {
//    //System.out.println("bg addBg");
//    String buyerStr = new String(bg, 0, len);
//    Value value = map.get(buyerStr);
//    if (value == null)
//      map.put(buyerStr, new Value((short) fileId, fileOff, 0));
//    else {
//      value.fileId = (short) fileId;
//      value.fileOff = fileOff;
//    }
//  }

//  public List<Tuple> getOrder(String bg) throws Exception {
//    Value value = map.get(bg);
//    if (value == null)
//      return new ArrayList<>();
//    //return table.getAll(value.blockNo);
//    return null;
//  }

//  public Tuple getBg(String bg) {
//    Value value = map.get(bg);
//    if (value == null)
//      return null;
//    return new Tuple(bgFiles.get(value.fileId), value.fileOff);
//  }
}