package com.alibaba.middleware.race.hash;

/**
 * Created by yfy on 7/13/16.
 * HashTable
 */
public class HashTable {

  private String indexFile, dataFile;

  public HashTable(String indexFile, String dataFile) {
    this.indexFile = indexFile;
    this.dataFile = dataFile;
  }

  public void add(byte[] key, int fileId, long fileOff) {

  }

}
