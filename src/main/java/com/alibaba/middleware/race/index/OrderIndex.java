package com.alibaba.middleware.race.index;

import com.alibaba.middleware.race.HashTable;
import com.alibaba.middleware.race.Tuple;
import com.alibaba.middleware.race.Util;

import java.util.List;

/**
 * Created by yfy on 7/24/16.
 * OrderIndex
 */
public class OrderIndex {

  private HashTable table;

  private final int BUCKET_NUM = 1 << 21;  // 2m

  private final int MASK = 0x1fffff;

  public OrderIndex(List<String> dataFiles, String indexFile) throws Exception {
    table = new HashTable(dataFiles, indexFile, BUCKET_NUM);
  }

  public void add(byte[] id, int fileId, long fileOff) throws Exception {
    table.add(id, Util.getHashCode(id) & MASK, fileId, fileOff);
  }

  public Tuple get(byte[] id) throws Exception {
    return table.get(id, Util.getHashCode(id) & MASK);
  }

}
