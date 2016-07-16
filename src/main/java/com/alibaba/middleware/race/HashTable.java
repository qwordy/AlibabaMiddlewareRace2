package com.alibaba.middleware.race;

import com.alibaba.middleware.race.cache.Cache;

import java.util.Arrays;
import java.util.List;

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
 * 1. fix-size key
 * KEY_SIZE, 4B, 8B
 * key, fileId, offset
 * 2. variable-size key
 * 2B,    keySize,  4B,    8B
 * keySize, key, fileId, offset
 *
 * order -> order
 * buyer -> order
 * good -> order
 * good -> good
 * buyer -> buyer
 */
public class HashTable {

  // number of buckets
  private final int SIZE;

  // also bucket size
  private static final int BLOCK_SIZE = 4096;

  private final boolean keySizeFixed, multiValue;

  private final int KEY_SIZE;

  private final int ENTRY_SIZE;

  private final int EXTRA_SIZE;

  // current number of blocks
  private int blockNums;

  private List<String> dataFiles;

  private String indexFile;

  private Cache cache;

  public static HashTable goodHashTable;

  /**
   *
   * @param dataFiles
   * @param indexFile
   * @param size number of buckets
   * @param keySize 0 when not fixed
   */
  public HashTable(List<String> dataFiles, String indexFile,
                   int size, int keySize, boolean multiValue, int extraSize) {
    this.dataFiles = dataFiles;
    this.indexFile = indexFile;

    SIZE = blockNums = size;

    if (keySize == 0) {
      keySizeFixed = false;
      KEY_SIZE = ENTRY_SIZE = 0;
    } else {
      keySizeFixed = true;
      KEY_SIZE = keySize;
      ENTRY_SIZE = KEY_SIZE + 12;
    }

    this.multiValue = multiValue;
    EXTRA_SIZE = extraSize;
    cache = Cache.getInstance();
  }

  /**
   * no replicated key
   * @param key
   * @param fileId
   * @param fileOffset
   * @throws Exception
   */
  public void add(byte[] key, int fileId, long fileOffset) throws Exception {
    if (multiValue)
      throw new Exception();
    if (keySizeFixed && key.length != KEY_SIZE)
      throw new Exception();

    // find the last block with the same hashcode in the chain
    byte[] bucket = new byte[BLOCK_SIZE];
    int blockNo = keyHashCode(key);  // current blockNo
    while (true) {
      cache.readBlock(indexFile, blockNo, bucket);
      int nextBlockNo = Util.byte2int(bucket, 0);
      if (nextBlockNo == 0)  // no next bucket
        break;
      blockNo = nextBlockNo;
    }

    // the next position in block to add entry
    int nextPos = Util.byte2int(bucket, 4);
    if (nextPos == 0) {
      nextPos = 8;
      System.arraycopy(Util.int2byte(8), 0, bucket, 4, 4);
    }

    // this bucket has no enough space to add entry
    if (keySizeFixed && nextPos + ENTRY_SIZE > BLOCK_SIZE ||
        !keySizeFixed && nextPos + key.length + 14> BLOCK_SIZE) {
      byte[] blockNumsBytes = Util.int2byte(blockNums);
      System.arraycopy(blockNumsBytes, 0, bucket, 0, 4);

      cache.writeBlock(indexFile, blockNo, bucket);

      Arrays.fill(bucket, (byte) 0);
      System.arraycopy(Util.int2byte(8), 0, bucket, 4, 4);
      nextPos = 8;

      blockNo = blockNums;
      blockNums++;
    }

    // Now bucket has enough space to add entry

    // write key size if need
    if (!keySizeFixed) {
      byte[] keySizeBytes = Util.short2byte(key.length);
      System.arraycopy(keySizeBytes, 0, bucket, nextPos, 2);
      nextPos += 2;
    }

    // key
    int keyLen = keySizeFixed ? KEY_SIZE : key.length;
    System.arraycopy(key, 0, bucket, nextPos, keyLen);
    nextPos += keyLen;

    // fileId
    byte[] fileIdBytes = Util.int2byte(fileId);
    System.arraycopy(fileIdBytes, 0, bucket, nextPos, 4);
    nextPos += 4;

    // fileOffset
    byte[] fileOffsetBytes = Util.long2byte(fileOffset);
    System.arraycopy(fileOffsetBytes, 0, bucket, nextPos, 8);
    nextPos += 8;

    // current size of block
    byte[] csizeBytes = Util.int2byte(nextPos);
    System.arraycopy(csizeBytes, 0, bucket, 4, 4);

    cache.writeBlock(indexFile, blockNo, bucket);
  }

  /**
   * allow one key maps to multiple value
   * @param key
   * @param fileId
   * @param fileOffset
   * @param extra extra information
   */
  public void addMulti(byte[] key, int fileId, long fileOffset, byte[] extra) {

  }

  private static class InnerAddr {
    public byte[] bucket;
    public int off;
    public InnerAddr(byte[] bucket, int off) {
      this.bucket = bucket;
      this.off = off;
    }
  }

  private InnerAddr innerGet(byte[] key) throws Exception {
    if (keySizeFixed && key.length != KEY_SIZE)
      throw new Exception();

    byte[] bucket = new byte[BLOCK_SIZE];
    int blockNo = keyHashCode(key);  // current blockNo
    while (true) {
      cache.readBlock(indexFile, blockNo, bucket);
      int size = Util.byte2int(bucket, 4);
      if (size == 0) size = 8;

      if (keySizeFixed) {
        for (int off = 8; off + ENTRY_SIZE <= size; off += ENTRY_SIZE) {
          if (Util.bytesEqual(bucket, off, key, 0, KEY_SIZE))  // find
            return new InnerAddr(bucket, off + KEY_SIZE);
        }
      } else {
        int off = 8;
        while (off < size) {
          int keyLen = Util.byte2short(bucket, off);
          off += 2;
          if (key.length == keyLen && Util.bytesEqual(key, 0, bucket, off, keyLen))
            return new InnerAddr(bucket, off + keyLen);
          off += keyLen;
          off += multiValue ? 4 : 12;
        }
      }

      blockNo = Util.byte2int(bucket, 0);
      if (blockNo == 0)
        return null;
    }
  }

  public Tuple get(byte[] key) throws Exception {
    if (multiValue)
      throw new Exception();

    InnerAddr addr = innerGet(key);
    if (addr == null)
      return null;
    int fileId = Util.byte2int(addr.bucket, addr.off);
    long fileOffset = Util.byte2long(addr.bucket, addr.off + 4);
    return new Tuple(dataFiles.get(fileId), fileOffset);
  }

  private int keyHashCode(byte[] key) {
    int h = 0;
    for (byte b : key) {
      h = 31 * h + b;
    }
    return Math.abs(h) % SIZE;
  }
}
