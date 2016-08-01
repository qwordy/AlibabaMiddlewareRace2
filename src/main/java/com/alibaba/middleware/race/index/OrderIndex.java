package com.alibaba.middleware.race.index;

import com.alibaba.middleware.race.*;

import java.io.RandomAccessFile;
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

  private int tableId, indexSize, indexSize1, indexSize2;

  public OrderIndex(List<String> dataFiles) {
    tables = new HashTable[2];
    this.dataFiles = dataFiles;
  }

  // 0..1
  public void setCurrentTable(int id, String indexFile, int indexSize) {
    tables[id] = new HashTable(dataFiles, indexFile,
        indexSize, Config.orderIndexBlockSize, 10);
    tableId = id;
    this.indexSize = indexSize;
    if (id == 0)
      indexSize1 = indexSize;
    else
      indexSize2 = indexSize;
  }

  public void finish() throws Exception {
    tables[tableId].writeFile();
  }

//  public void setTable0DirectMemory(ByteBuffer buffer1, ByteBuffer buffer2) {
//    tables[0].setOrderTable0DirectMemory(buffer1, buffer2);
//  }

  // id.length == 5
  public void add(byte[] id, int fileId, long fileOff) throws Exception {
    int hash = Util.bytesHash(id) % indexSize;
    tables[tableId].add(id, hash, fileId, fileOff);
  }

  public Tuple get(byte[] id) throws Exception {
    int rawHash = Util.bytesHash(id);
    int hash = rawHash % indexSize1;
    Tuple tuple = tables[0].get(id, hash);
    if (tuple == null) {
      hash = rawHash % indexSize2;
      tuple = tables[1].get(id, hash);
    }
    return tuple;
  }
}
