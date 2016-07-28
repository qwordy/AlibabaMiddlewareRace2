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

  private HashTable table;

  public OrderIndex(List<String> dataFiles, String indexFile,
                    WriteBuffer writeBuffer) throws Exception {

    RandomAccessFile fd = new RandomAccessFile(indexFile, "rw");
    writeBuffer.setFd(fd);
    table = new HashTable(dataFiles, fd, Config.orderIndexSize,
        Config.orderIndexBlockSize, 10, writeBuffer);
  }

  // id.length == 5
  public void add(byte[] id, int fileId, long fileOff) throws Exception {
    //System.out.println("order add");
    int hash = Util.bytesHash(id) & 0x1fffff;
    table.add(id, hash, fileId, fileOff);
  }

  public Tuple get(byte[] id) throws Exception {
    int hash = Util.bytesHash(id) & 0x1fffff;
    return table.get(id, hash);
  }
}
