package com.alibaba.middleware.race.index;

import com.alibaba.middleware.race.HashTable;
import com.alibaba.middleware.race.Tuple;
import com.alibaba.middleware.race.Util;
import com.alibaba.middleware.race.WriteBuffer;

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

  private HashTable table;

  public OrderIndex(List<String> dataFiles, String indexFile,
                    int bucketNum, int bucketSize,
                    WriteBuffer writeBuffer) throws Exception {

    RandomAccessFile fd = new RandomAccessFile(indexFile, "rw");
    table = new HashTable(dataFiles, fd, )
  }

  //private HashTable[] table;

  // each table bucket num
  //private final int BUCKET_NUM = 1 << 17;

  // 21
  //private final int BIG_MASK = 0x1fffff;

  // 17
  //private final int SMALL_MASK = 0x1ffff;

//  public OrderIndex(List<String> dataFiles, String indexFile,
//                    WriteBuffer writeBuffer) throws Exception {
//
//    table = new HashTable[16];
//    for (int i = 0; i < 16; i++) {
//      RandomAccessFile fd = new RandomAccessFile(indexFile + i, "rw");
//      fd.setLength(1 << 29);
//      writeBuffer.addFd(i, fd);
//      table[i] = new HashTable(dataFiles, i, fd, BUCKET_NUM, writeBuffer);
//    }
//  }
//
//  public void add(byte[] id, int fileId, long fileOff) throws Exception {
//    //System.out.println("order add");
//    int hash = Util.bytesHash(id) & BIG_MASK;
//    table[hash >> 17].add(id, hash & SMALL_MASK, fileId, fileOff);
//  }
//
//  public Tuple get(byte[] id) throws Exception {
//    int hash = Util.bytesHash(id) & BIG_MASK;
//    return table[hash >> 17].get(id, hash & SMALL_MASK);
//  }
}
