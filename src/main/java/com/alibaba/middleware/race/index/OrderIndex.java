package com.alibaba.middleware.race.index;

import com.alibaba.middleware.race.Config;
import com.alibaba.middleware.race.HashTable;
import com.alibaba.middleware.race.Tuple;
import com.alibaba.middleware.race.Util;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by yfy on 7/24/16.
 * OrderIndex
 * 16 hashTables, each 500m, total 8g
 * 1 << 21 = 2097152
 * 1 << 17 = 131072
 * 1 << 14 = 16384
 */
public class OrderIndex {

  private HashTable[] tables;

  private List<String> dataFiles;

  private int tableId;

  public OrderIndex(List<String> dataFiles) {
    tables = new HashTable[2];
    this.dataFiles = dataFiles;
  }

  // 0..1
  public void setCurrentTable(int id, String indexFile) {
    tables[id] = new HashTable(dataFiles, indexFile,
        Config.orderIndexSize, Config.orderIndexBlockSize, 10);
    tableId = id;
  }

  public void finish() throws Exception {
    tables[tableId].writeFile();
  }

  public void setTable1DirectMemory(ByteBuffer buffer1, ByteBuffer buffer2) {
    tables[1].setOrderTable1DirectMemory(buffer1, buffer2);
  }

  // id.length == 5
  public void add(byte[] id, int fileId, long fileOff) throws Exception {
    int hash = Util.bytesHash(id) % Config.orderIndexSize;
    tables[tableId].add(id, hash, fileId, fileOff);
  }

  public Tuple get(byte[] id) throws Exception {
    int hash = Util.bytesHash(id) % Config.orderIndexSize;
    Tuple tuple = tables[1].get(id, hash);
    if (tuple == null)
      tuple = tables[0].get(id, hash);
    return tuple;
  }
}
