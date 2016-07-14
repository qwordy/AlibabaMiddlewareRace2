package com.alibaba.middleware.race.hash;

import com.alibaba.middleware.race.Util;

/**
 * Created by yfy on 7/13/16.
 * HashTable.
 *
 * Bucket structure:
 * 8 byte address of next bucket of the same hash code
 * many entrys
 *
 * Entry structure:
 * key, fileId, offset
 *
 */
public class HashTable {

  private String indexFile;

  private final int SIZE = 10;

  private final int BUCKET_SIZE = 60;

  private final int KEY_SIZE = 8;

  public HashTable(String indexFile) {
    this.indexFile = indexFile;

  }

  public void add(long key, int fileId, long fileOffset) {
    System.out.println("add " + key + " " + fileOffset);
    add(Util.long2byte(key), fileId, fileOffset);
  }

  // pay attention to key.length
  public void add(byte[] key, int fileId, long fileOffset) {
    
  }

  private int keyHashCode(byte[] key) {

    return 0;
  }
}
