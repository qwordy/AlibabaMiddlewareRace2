package com.alibaba.middleware.race.index;

import com.alibaba.middleware.race.*;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by yfy on 7/24/16.
 * Buyer or good index
 */
public class BgIndex {

  private Map<String, Value> map;

  // bg 2 o
  private HashTable[] tables;

  private int tableId;

  // bg 2 bg
  private HashTable table;

  private List<String> orderFiles, bgFiles;

  private int size, blockSize, count;

  public BgIndex(List<String> orderFiles, List<String> bgFiles,
                 int size, int blockSize) throws Exception {

    this.orderFiles = orderFiles;
    this.bgFiles = bgFiles;
    this.size = size;
    this.blockSize = blockSize;
    tables = new HashTable[3];

    //map = new ConcurrentHashMap<>(size);
    //RandomAccessFile fd = new RandomAccessFile(bg2oIndexFile, "rw");
    //table = new HashTable(orderFiles, fd, size, blockSize, 5, writeBuffer);
  }

  public void setBgTable(int size, int blockSize) throws Exception {
    table = new HashTable(bgFiles, null, size, blockSize, 29);
  }

  // 0..2
  public void setCurrentTable(int id, String indexFile) throws Exception {
    tables[id] = new HashTable(orderFiles, indexFile, size, blockSize, 5);
    tableId = id;
  }

  public void finish(int id) throws Exception {
    tables[id].writeFile();
  }

  public void addOrder(byte[] bg, int len, int fildId, long fildOff)
      throws Exception {



    //System.out.println("bg addOrder");
    String buyerStr = new String(bg, 0, len);
    Value value = map.get(buyerStr);
    int blockNo;
    if (value == null) {
      blockNo = count;
      map.put(buyerStr, new Value((short) 0, 0, count));
      count++;
    } else {
      blockNo = value.blockNo;
      value.orderNum++;
    }
    //table.add(null, blockNo, fildId, fildOff);
  }

  public void addBg(byte[] bg, int len, int fileId, long fileOff) {
    //System.out.println("bg addBg");
    String buyerStr = new String(bg, 0, len);
    Value value = map.get(buyerStr);
    if (value == null)
      map.put(buyerStr, new Value((short) fileId, fileOff, 0));
    else {
      value.fileId = (short) fileId;
      value.fileOff = fileOff;
    }
  }

  public List<Tuple> getOrder(String bg) throws Exception {
    Value value = map.get(bg);
    if (value == null)
      return new ArrayList<>();
    //return table.getAll(value.blockNo);
    return null;
  }

  public Tuple getBg(String bg) {
    Value value = map.get(bg);
    if (value == null)
      return null;
    return new Tuple(bgFiles.get(value.fileId), value.fileOff);
  }

  public void printInfo(String tag) {
    int[] nums = new int[12];
    int max = 0;
    for (Value value : map.values()) {
      nums[value.orderNum / 50]++;
      if (value.orderNum > max)
        max = value.orderNum;
    }

    System.out.println("[yfy] " + tag + " max order num: " + max);
    System.out.print("[yfy] dist: ");
    for (int i = 0; i < 12; i++)
      System.out.print(nums[i] + " ");
    System.out.println();
  }

  private static class Value {
    short fileId;  // bg file id
    long fileOff;  // bg file off
    int orderNum;
    int blockNo;

    Value(short fileId, long fileOff, int blockNo) {
      this.fileId = fileId;
      this.fileOff = fileOff;
      this.blockNo = blockNo;
      orderNum = 1;
    }
  }
}
