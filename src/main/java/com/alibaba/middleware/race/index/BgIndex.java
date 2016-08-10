package com.alibaba.middleware.race.index;

import com.alibaba.middleware.race.BgBytes;
import com.alibaba.middleware.race.HashTable;
import com.alibaba.middleware.race.Tuple;
import com.alibaba.middleware.race.Util;
import com.alibaba.middleware.race.result.BuyerResult;
import com.alibaba.middleware.race.result.GoodResult;

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
    boolean find = bgTable.getBg(bg, len, bgBytes);
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
    boolean find = bgTable.getBg(bg, len, bgBytes);
    bgBytes.block[bgBytes.off] = (byte) fileId;
    Util.longToByte4(fileOff, bgBytes.block, bgBytes.off + 1);
    if (!find)
      Util.int2byte3(0xffffff, bgBytes.block, bgBytes.off + 5);
  }

  public List<Tuple> getOrder(String bg, boolean buyer) throws Exception {
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
    return orderTable.getAll(bgId, buyer);
  }

  public void saveBuyerAll(List<BuyerResult> list, String bg) throws Exception {
    int bgId = bgTable.getBgId(bg.getBytes(), bg.length());
    orderTable.saveBuyerAll(list, bgId);
  }

  public void saveGoodAll(List<GoodResult> list, String bg) throws Exception {
    int bgId = bgTable.getBgId(bg.getBytes(), bg.length());
    orderTable.saveGoodAll(list, bgId);
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
