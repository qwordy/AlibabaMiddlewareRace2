package com.alibaba.middleware.race.index;

import com.alibaba.middleware.race.*;

import java.io.RandomAccessFile;
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
    tables = new HashTable[3];
    this.dataFiles = dataFiles;
  }

  // 0..2
  public void setCurrentTable(int id, String indexFile) throws Exception {
    tables[id] = new HashTable(dataFiles, indexFile,
        Config.orderIndexSize, Config.orderIndexBlockSize, 10);
    tableId = id;
  }

  public void finish(int id) throws Exception {
    tables[id].writeFile();
  }

  // id.length == 5
  public void add(byte[] id, int fileId, long fileOff) throws Exception {
    //System.out.println("order add");
    int hash = Util.bytesHash(id) % Config.orderIndexSize;
    tables[tableId].add(id, hash, fileId, fileOff);
  }

  public Tuple get(byte[] id) throws Exception {
    int hash = Util.bytesHash(id) % Config.orderIndexSize;
    Tuple tuple = tables[0].get(id, hash);
    if (tuple == null)
      tuple = tables[1].get(id, hash);
    if (tuple == null)
      tuple = tables[2].get(id, hash);
    return tuple;
  }
}
