package com.alibaba.middleware.race.hash;

import com.alibaba.middleware.race.Util;
import com.alibaba.middleware.race.cache.Cache;

/**
 * Created by yfy on 7/13/16.
 * HashTable.
 * <p>
 * Bucket structure:
 * 1. 4 byte, blockNo of next bucket of the same hash code
 * if 0, no next bucket
 * 2. 4 byte, current size(total num of bytes) of block
 * if 0, no entrys
 * 3. many entrys
 * <p>
 * 4B   4B
 * next size entry1 entry2 ... entryn
 * <p>
 * Entry structure:
 * 4B      8B
 * key, fileId, offset
 */
public class HashTable {

  // number of buckets
  private final int SIZE = 10;

  // also bucket size
  private final int BLOCK_SIZE = 4096;

  private final int KEY_SIZE = 8;

  private final int ENTRY_SIZE = KEY_SIZE + 12;

  // current number of blocks
  private int blockNums;

  private String indexFile;

  private Cache cache;

  public HashTable(String indexFile) {
    this.indexFile = indexFile;
    blockNums = SIZE;
    cache = Cache.getInstance();
  }

  public void add(long key, int fileId, long fileOffset) throws Exception {
    System.out.println("add " + key + " " + fileId + " " + fileOffset);
    add(Util.long2byte(key), fileId, fileOffset);
  }

  // pay attention to key.length
  public void add(byte[] key, int fileId, long fileOffset) throws Exception {
    byte[] bucket = new byte[BLOCK_SIZE];
    int blockNo = keyHashCode(key);  // current blockNo
    while (true) {
      cache.readBlock(indexFile, blockNo, bucket);
      if (Util.byte2int(bucket, 0) == 0)  // no next bucket
        break;
      else
        blockNo = Util.byte2int(bucket, 0);
    }

    // the next position in block to add entry
    int nextPos = Util.byte2int(bucket, 4);
    // this bucket has no enough space to add entry
    if (nextPos + ENTRY_SIZE > BLOCK_SIZE) {
      byte[] blockNumsBytes = Util.int2byte(blockNums);
      System.arraycopy(blockNumsBytes, 0, bucket, 0, 4);

      cache.writeBlock(indexFile, blockNo, bucket);

      bucket = new byte[BLOCK_SIZE];
      System.arraycopy(Util.int2byte(8), 0, bucket, 4, 4);

      blockNo = blockNums;
      blockNums++;
    }

    // Now bucket has enough space to add entry

    // key
    System.arraycopy(key, 0, bucket, nextPos, KEY_SIZE);

    // fileId
    byte[] fileIdBytes = Util.int2byte(fileId);
    System.arraycopy(fileIdBytes, 0, bucket, nextPos + KEY_SIZE, 4);

    // fileOffset
    byte[] fileOffsetBytes = Util.long2byte(fileOffset);
    System.arraycopy(fileOffsetBytes, 0, bucket, nextPos + KEY_SIZE + 4, 8);

    nextPos += ENTRY_SIZE;
    byte[] offsetBytes = Util.int2byte(nextPos);
    System.arraycopy(offsetBytes, 0, bucket, 4, 4);

    cache.writeBlock(indexFile, blockNo, bucket);

  }

  public void get(byte[] key) {

  }

  private int keyHashCode(byte[] key) {
    int h = 0;
    for (byte b : key) {
      h = 31 * h + b;
    }
    return Math.abs(h) % SIZE;
  }
}
