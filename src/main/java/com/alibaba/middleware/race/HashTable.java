package com.alibaba.middleware.race;

import java.io.RandomAccessFile;
import java.util.ArrayList;
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
 * key, fileId, fileOffset
 * 2. variable-size key
 * 2B,    keySize,  4B,    8B
 * keySize, key, fileId, offset
 * <p>
 * If multivalue
 * entry:
 * (keysize), key, blockNo
 * subbucket:
 * next bucket, size, entrys
 * entry:
 * fileId, fileOffset, (extraInfo)
 * <p>
 * order -> order
 * buyer -> order
 * good -> order
 * good -> good
 * buyer -> buyer
 */
public class HashTable {

  // also bucket size
  private static final int BLOCK_SIZE = 4096;

  // 2 ^ BIT = BLOCK_SIZE
  private static final int BIT = 12;

  private static final int ENTRY_SIZE = 14;

  // current number of blocks
  private int blockNums;

  private Meta[] bucketMetas;

  private List<String> dataFiles;

  private int indexFileId;

  private RandomAccessFile fd;

  private byte[] entryBuf;

  private WriteBuffer writeBuffer;

  public HashTable(List<String> dataFiles, int indexFileId, RandomAccessFile fd,
                   int size, WriteBuffer writeBuffer) throws Exception {

    this.dataFiles = dataFiles;
    this.indexFileId = indexFileId;
    this.fd = fd;
    blockNums = size;
    this.writeBuffer = writeBuffer;

    bucketMetas = new Meta[size];
//    for (int i = 0; i < size; i++)
//      bucketMetas[i] = new Meta(0, 6);

    entryBuf = new byte[ENTRY_SIZE];
  }

  public void add(byte[] data, int blockNo, int fileId, long fileOff) throws Exception {
    Meta meta = bucketMetas[blockNo];
    if (meta == null)
      meta = bucketMetas[blockNo] = new Meta();
    // find the last bucket in the chain
    while (meta.next > 0) {
      blockNo = meta.next;
      meta = meta.nextMeta;
    }
    // no enough space in bucket
    if (meta.size + ENTRY_SIZE > BLOCK_SIZE) {
      meta.next = blockNums++;
      meta.nextMeta = new Meta();
      blockNo = meta.next;
      meta = meta.nextMeta;
    }

    // data(key)
    System.arraycopy(data, 0, entryBuf, 0, 8);
    // fileId
    System.arraycopy(Util.short2byte(fileId), 0, entryBuf, 8, 2);
    // fildOff
    System.arraycopy(Util.longTo4Byte(fileOff), 0, entryBuf, 10, 4);

    writeBuffer.add(indexFileId,
        new WriteRequest(entryBuf, (((long) blockNo) << BIT) + meta.size));
    meta.size += ENTRY_SIZE;
  }

  public Tuple get(byte[] key, int blockNo) throws Exception {
    byte[] buf = new byte[BLOCK_SIZE];
    Meta meta = bucketMetas[blockNo];
    if (meta == null)
      meta = bucketMetas[blockNo] = new Meta();
    while (true) {
      synchronized (fd) {
        fd.seek(((long) blockNo) << BIT);
        fd.read(buf);
      }
      int size = meta.size;
      for (int off = 6; off + ENTRY_SIZE <= size; off += ENTRY_SIZE) {
        if (Util.bytesEqual(buf, off, key, 0, 8)) {
          int fileId = Util.byte2short(buf, off + 8);
          long fileOff = Util.byte4ToLong(buf, off + 10);
          return new Tuple(dataFiles.get(fileId), fileOff);
        }
      }
      blockNo = meta.next;
      if (blockNo == 0)
        return null;
      meta = meta.nextMeta;
    }
  }

  public List<Tuple> getAll(int blockNo) throws Exception {
    List<Tuple> list = new ArrayList<>();
    byte[] buf = new byte[BLOCK_SIZE];
    Meta meta = bucketMetas[blockNo];
    if (meta == null)
      meta = bucketMetas[blockNo] = new Meta();
    while (true) {
      synchronized (fd) {
        fd.seek(((long) blockNo) << BIT);
        fd.read(buf);
      }
      int size = meta.size;
      for (int off = 6; off + ENTRY_SIZE <= size; off += ENTRY_SIZE) {
        long data = Util.byte2long(buf, off);
        int fileId = Util.byte2short(buf, off + 8);
        long fileOff = Util.byte4ToLong(buf, off + 10);
        list.add(new Tuple(dataFiles.get(fileId), fileOff, data));
      }
      blockNo = meta.next;
      if (blockNo == 0)
        return list;
      meta = meta.nextMeta;
    }
  }

  private static class Meta {
    int next, size = 6;
    Meta nextMeta;
  }

//  private InnerAddr innerGet(byte[] key) throws Exception {
//    if (keySizeFixed && key.length != KEY_SIZE)
//      throw new Exception();
//
//    byte[] bucket;
//    int blockNo = keyHashCode(key);  // current blockNo
//    while (true) {
//      bucket = cache.readBlock(indexFile, blockNo);
//      int size = Util.byte2int(bucket, 4);
//      if (size == 0) size = 8;
//
//      if (keySizeFixed) {
//        for (int off = 8; off + ENTRY_SIZE <= size; off += ENTRY_SIZE) {
//          if (Util.bytesEqual(bucket, off, key, 0, KEY_SIZE))  // find
//            return new InnerAddr(bucket, blockNo, off + KEY_SIZE, true);
//        }
//      } else {
//        int off = 8;
//        while (off < size) {
//          int keyLen = Util.byte2short(bucket, off);
//          off += 2;
//          if (key.length == keyLen && Util.bytesEqual(key, 0, bucket, off, keyLen))
//            return new InnerAddr(bucket, blockNo, off + keyLen, true);
//          off += keyLen;
//          off += multiValue ? 4 : 12;
//        }
//      }
//
//      int newBLockNo = Util.byte2int(bucket, 0);
//      if (newBLockNo == 0)
//        return new InnerAddr(bucket, blockNo, 0, false);
//      blockNo = newBLockNo;
//    }
//  }

//  public Tuple get(byte[] key) throws Exception {
//    if (multiValue)
//      throw new Exception();
//
//    InnerAddr addr = innerGet(key);
//    if (!addr.find)
//      return null;
//    int fileId = Util.byte2int(addr.bucket, addr.off);
//    long fileOffset = Util.byte2long(addr.bucket, addr.off + 4);
//    return new Tuple(dataFiles.get(fileId), fileOffset);
//  }

//  public List<Tuple> getMulti(byte[] key, TupleFilter filter) throws Exception {
//    if (!multiValue)
//      throw new Exception();
//
//    List<Tuple> list = new ArrayList<>();
//
//    InnerAddr addr = innerGet(key);
//    if (!addr.find)
//      return list;
//
//    int blockNo = Util.byte2int(addr.bucket, addr.off);
//    byte[] bucket;
//
//    while (true) {
//      bucket = cache.readBlock(indexFile, blockNo);
//      int size = Util.byte2int(bucket, 4);
//
//      for (int off = 8; off + 12 + EXTRA_SIZE <= size; off += 12 + EXTRA_SIZE) {
//        int fileId = Util.byte2int(bucket, off);
//        long fileOffset = Util.byte2long(bucket, off + 4);
//        if (EXTRA_SIZE == 0)
//          list.add(new Tuple(dataFiles.get(fileId), fileOffset));
//        else if (EXTRA_SIZE == 8) {
//          if (filter == null) {
//            list.add(new Tuple(dataFiles.get(fileId), fileOffset,
//                Util.byte2long(bucket, off + 12)));
//          } else {
//            long time = Util.byte2long(bucket, off + 12);
//            if (filter.test(time))
//              list.add(new Tuple(dataFiles.get(fileId), fileOffset, time));
//          }
//        }
//      }
//
//      blockNo = Util.byte2int(bucket, 0);
//      if (blockNo == 0)
//        return list;
//    }
//  }

//  private int keyHashCode(byte[] key) {
//    int h = 0;
//    for (byte b : key) {
//      h = 31 * h + b;
//    }
//    return Math.abs(h) % SIZE;
//  }

  //  /**
//   * @param dataFiles
//   * @param indexFile
//   * @param size      number of buckets
//   * @param keySize   0 when not fixed
//   */
//  public HashTable(List<String> dataFiles, String indexFile,
//                   int size, int keySize, boolean multiValue, int extraSize) {
//    this.dataFiles = dataFiles;
//    this.indexFile = indexFile;
//
//    SIZE = blockNums = size;
//
//    if (keySize == 0) {
//      keySizeFixed = false;
//      KEY_SIZE = ENTRY_SIZE = 0;
//    } else {
//      keySizeFixed = true;
//      KEY_SIZE = keySize;
//      ENTRY_SIZE = KEY_SIZE + 12;
//    }
//
//    this.multiValue = multiValue;
//    EXTRA_SIZE = extraSize;
//    cache = ConcurrentCache.getInstance();
//  }

//  public void addMulti(byte[] key, int fileId, long fileOffset, byte[] extra) throws Exception {
//    if (!multiValue)
//      throw new Exception();
//    if (keySizeFixed && key.length != KEY_SIZE)
//      throw new Exception();
//    if (extra == null) {
//      if (EXTRA_SIZE != 0)
//        throw new Exception();
//    } else {
//      if (extra.length != EXTRA_SIZE)
//        throw new Exception();
//    }
//
//    InnerAddr addr = innerGet(key);
//    byte[] bucket = addr.bucket;
//    int blockNo = addr.blockNo;
//    if (addr.find) {
//      blockNo = Util.byte2int(bucket, addr.off);
//      // find the last block in the chain
//      while (true) {
//        bucket = cache.readBlock(indexFile, blockNo);
//        int nextBlockNo = Util.byte2int(bucket, 0);
//        if (nextBlockNo == 0)
//          break;
//        blockNo = nextBlockNo;
//      }
//      addValueMulti(bucket, blockNo, fileId, fileOffset, extra);
//    } else {
//      // the next position in block to add entry
//      int nextPos = Util.byte2int(bucket, 4);
//      if (nextPos == 0) {
//        nextPos = 8;
//        System.arraycopy(Util.int2byte(8), 0, bucket, 4, 4);
//      }
//
//      if (!keySizeFixed) {
//        // no enough space in this bucket
//        if (nextPos + 2 + key.length + 4 > BLOCK_SIZE) {
//          byte[] blockNumsBytes = Util.int2byte(blockNums);
//          System.arraycopy(blockNumsBytes, 0, bucket, 0, 4);
//
//          cache.writeBlock(indexFile, blockNo, bucket);
//
//          bucket = new byte[BLOCK_SIZE];
//          System.arraycopy(Util.int2byte(8), 0, bucket, 4, 4);
//          nextPos = 8;
//
//          blockNo = blockNums;
//          blockNums++;
//        }
//
//        // key size
//        byte[] keySizeBytes = Util.short2byte(key.length);
//        System.arraycopy(keySizeBytes, 0, bucket, nextPos, 2);
//        nextPos += 2;
//
//        // key
//        System.arraycopy(key, 0, bucket, nextPos, key.length);
//        nextPos += key.length;
//
//        // point to a block which contains values of the same key
//        byte[] blockNumsBytes = Util.int2byte(blockNums);
//        System.arraycopy(blockNumsBytes, 0, bucket, nextPos, 4);
//        nextPos += 4;
//
//        // current size of block
//        byte[] csizeBytes = Util.int2byte(nextPos);
//        System.arraycopy(csizeBytes, 0, bucket, 4, 4);
//
//        cache.writeBlock(indexFile, blockNo, bucket);
//
//        // init new bucket
//        bucket = new byte[BLOCK_SIZE];
//        System.arraycopy(Util.int2byte(8), 0, bucket, 4, 4);
//        blockNo = blockNums;
//        blockNums++;
//
//        addValueMulti(bucket, blockNo, fileId, fileOffset, extra);
//
//      } else {
//      }
//    }
//  }
//
//  // only used in addMulti
//  // add fileId, fileOffset, (extra)
//  private void addValueMulti(byte[] bucket, int blockNo, int fileId, long fileOffset, byte[] extra)
//      throws Exception {
//
//    int nextPos = Util.byte2int(bucket, 4);
//
//    // block has no enough space
//    if (nextPos + 12 + EXTRA_SIZE > BLOCK_SIZE) {
//      byte[] blockNumsBytes = Util.int2byte(blockNums);
//      System.arraycopy(blockNumsBytes, 0, bucket, 0, 4);
//
//      cache.writeBlock(indexFile, blockNo, bucket);
//
//      bucket = new byte[BLOCK_SIZE];
//      System.arraycopy(Util.int2byte(8), 0, bucket, 4, 4);
//      nextPos = 8;
//
//      blockNo = blockNums;
//      blockNums++;
//    }
//
//    // fileId
//    byte[] fileIdBytes = Util.int2byte(fileId);
//    System.arraycopy(fileIdBytes, 0, bucket, nextPos, 4);
//    nextPos += 4;
//
//    // fileOffset
//    byte[] fileOffsetBytes = Util.long2byte(fileOffset);
//    System.arraycopy(fileOffsetBytes, 0, bucket, nextPos, 8);
//    nextPos += 8;
//
//    // extra
//    if (EXTRA_SIZE > 0) {
//      System.arraycopy(extra, 0, bucket, nextPos, EXTRA_SIZE);
//      nextPos += EXTRA_SIZE;
//    }
//
//    // current size of block
//    byte[] csizeBytes = Util.int2byte(nextPos);
//    System.arraycopy(csizeBytes, 0, bucket, 4, 4);
//
//    cache.writeBlock(indexFile, blockNo, bucket);
//  }
//
//  private static class InnerAddr {
//    byte[] bucket;
//    int blockNo;
//    int off;  // start position of value
//    boolean find;  // find the key
//
//    InnerAddr(byte[] bucket, int blockNo, int off, boolean find) {
//      this.bucket = bucket;
//      this.blockNo = blockNo;
//      this.off = off;
//      this.find = find;
//    }
//  }
}
