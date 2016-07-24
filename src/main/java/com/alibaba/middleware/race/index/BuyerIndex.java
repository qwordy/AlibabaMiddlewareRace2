package com.alibaba.middleware.race.index;

import com.alibaba.middleware.race.HashTable;

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

  public BuyerIndex(List<String> orderFiles, String b2oIndexFile, List<String> buyerFiles)
      throws Exception {
    map = new ConcurrentHashMap<>();
    table = new HashTable(orderFiles, b2oIndexFile, 8000000);
  }

  public void addOrder(String buyer, int fildId, long fildOff, long time) {
    
  }

  public void addBuyer(String buyer, int fileId, long fileOff) {
    Value value = map.get(buyer);
    if (value == null)
      map.put(buyer, new Value((short) fileId, fileOff, 0));
    else {
      value.fileId = (short) fileId;
      value.fileOff = fileOff;
    }
  }

  private static class Value {
    short fileId;
    long fileOff;
    int blockNo;
    public Value(short fileId, long fileOff, int blockNo) {
      this.fileId = fileId;
      this.fileOff = fileOff;
      this.blockNo = blockNo;
    }
  }

}
