package com.alibaba.middleware.race.index;

import com.alibaba.middleware.race.HashTable;
import com.alibaba.middleware.race.Tuple;
import com.alibaba.middleware.race.Util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yfy on 7/24/16.
 * OrderIndex
 */
public class OrderIndex {

  private HashTable hashTable;

  private final int BUCKET_NUM = 1 << 21;  // 2m

  private final int MASK = 0x1fffff;

  public OrderIndex(List<String> dataFiles, String indexFile) throws Exception {
    hashTable = new HashTable(dataFiles, indexFile, BUCKET_NUM);
  }

  public void add(byte[] id, int fileId, long fileOff) throws Exception {
    hashTable.add(id, Util.getHashCode(id) & MASK, fileId, fileOff);
  }

  public Tuple get(byte[] id) throws Exception {
    return hashTable.get(id, Util.getHashCode(id) & MASK);
  }

}
