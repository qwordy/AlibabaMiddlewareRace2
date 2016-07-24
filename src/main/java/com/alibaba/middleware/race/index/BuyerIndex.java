package com.alibaba.middleware.race.index;

import com.alibaba.middleware.race.HashTable;
import com.alibaba.middleware.race.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by yfy on 7/24/16.
 * BuyerIndex
 */
public class BuyerIndex {

  private Map<String, Value> map;

  private HashTable table;

  private int count;

  private List<String> buyerFiles;

  public BuyerIndex(List<String> orderFiles, String b2oIndexFile, List<String> buyerFiles)
      throws Exception {

    this.buyerFiles = buyerFiles;
    map = new ConcurrentHashMap<>();
    table = new HashTable(orderFiles, b2oIndexFile, 8000000);
  }

  public void addOrder(byte[] buyer, int fildId, long fildOff, byte[] time)
      throws Exception {

    String buyerStr = new String(buyer);
    Value value = map.get(buyerStr);
    int blockNo;
    if (value == null) {
      blockNo = count;
      map.put(buyerStr, new Value((short) 0, 0, count));
      count++;
    } else {
      blockNo = value.blockNo;
    }
    table.add(time, blockNo, fildId, fildOff);
  }

  public void addBuyer(byte[] buyer, int fileId, long fileOff) {
    String buyerStr = new String(buyer);
    Value value = map.get(buyerStr);
    if (value == null)
      map.put(buyerStr, new Value((short) fileId, fileOff, 0));
    else {
      value.fileId = (short) fileId;
      value.fileOff = fileOff;
    }
  }

  public List<Tuple> getOrder(String buyer) throws Exception {
    Value value = map.get(buyer);
    if (value == null)
      return new ArrayList<>();
    return table.getAll(value.blockNo);
  }

  public Tuple getBuyer(String buyer) {
    Value value = map.get(buyer);
    if (value == null)
      return null;
    return new Tuple(buyerFiles.get(value.fileId), value.fileOff);
  }

  private static class Value {
    short fileId;  // buyer file id
    long fileOff;  // buyer file off
    int blockNo;

    public Value(short fileId, long fileOff, int blockNo) {
      this.fileId = fileId;
      this.fileOff = fileOff;
      this.blockNo = blockNo;
    }
  }

}
