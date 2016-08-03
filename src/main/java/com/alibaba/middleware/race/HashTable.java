package com.alibaba.middleware.race;

import com.alibaba.middleware.race.result.BuyerResult;
import com.alibaba.middleware.race.result.GoodResult;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
 *
 * 4, 2,    1, 4, (5)
 */
public class HashTable {

  private final int SIZE;

  private final int BLOCK_SIZE;

  // 2 ^ BIT = BLOCK
  //private final int BIT;

  private final int ENTRY_SIZE;

  // current number of blocks
  private int blockNum;

  private List<String> dataFiles;

  private String indexFile;

  private RandomAccessFile fd;

  private byte[][] memory;

  private List<byte[]> memoryExt;

  private ByteBuffer byteBuffer1, byteBuffer2;

  public HashTable(List<String> dataFiles, String indexFile,
      int size, int blockSize, int entrySize) {

    this.dataFiles = dataFiles;
    this.indexFile = indexFile;
    SIZE = blockNum = size;
    BLOCK_SIZE = blockSize;
    ENTRY_SIZE = entrySize;

    memory = new byte[size][];
    for (int i = 0; i < size; i++)
      memory[i] = new byte[BLOCK_SIZE];
    memoryExt = new ArrayList<>();
  }

  // key.length == 5
  public void add(byte[] key, int blockNo, int fileId, long fileOff) {

    byte[] block;
    if (blockNo < SIZE)
      block = memory[blockNo];
    else
      block = memoryExt.get(blockNo - SIZE);

    // find the last bucket in the chain
    while (Util.byte2int(block, 0) > 0) {
      blockNo = Util.byte2int(block, 0);
      if (blockNo < SIZE)
        block = memory[blockNo];
      else
        block = memoryExt.get(blockNo - SIZE);
    }
    // no enough space in bucket
    if (Util.byte2short(block, 4) + ENTRY_SIZE > BLOCK_SIZE) {
      blockNo = blockNum++;
      Util.int2byte(blockNo, block, 0);
      block = new byte[BLOCK_SIZE];
      memoryExt.add(block);
    }

    int nextPos = Util.byte2short(block, 4);
    if (nextPos == 0) nextPos = 6;

    // fileId
    block[nextPos] = (byte) fileId;
    nextPos++;
    // fileOff
    Util.longToByte4(fileOff, block, nextPos);
    nextPos += 4;
    // key
    if (key != null) {
      System.arraycopy(key, 0, block, nextPos, 5);
      nextPos += 5;
    }

    Util.short2byte(nextPos, block, 4);
  }

  public void setOrderTable1DirectMemory(ByteBuffer buffer1, ByteBuffer buffer2) {
    byteBuffer1 = buffer1;
    byteBuffer2 = buffer2;
  }

  // get order, entry size 10
  public Tuple get(byte[] key, int blockNo) throws Exception {
    byte[] block = new byte[BLOCK_SIZE];
    while (true) {
      if (byteBuffer1 != null) {
        //System.out.println("bytebuffer" + blockNo);
        int b1bn = Config.orderIndexBuffer1BlockNum;
        if (blockNo < b1bn) {
          synchronized (byteBuffer1) {
            byteBuffer1.position(blockNo * BLOCK_SIZE);
            byteBuffer1.get(block);
          }
        } else {
          synchronized (byteBuffer2) {
            byteBuffer2.position((blockNo - b1bn) * BLOCK_SIZE);
            byteBuffer2.get(block);
          }
        }
      } else {
        //System.out.println("disk" + blockNo);
        synchronized (fd) {
          fd.seek(((long) blockNo) * BLOCK_SIZE);
          fd.read(block);
        }
      }
      int size = Util.byte2short(block, 4);
      if (size == 0) size = 6;
      for (int off = 6; off + 10 <= size; off += 10) {
        if (Util.bytesEqual(block, off + 5, key, 0, 5)) {
          int fileId = block[off] & 0xff;
          long fileOff = Util.byte4ToLong(block, off + 1);
          return new Tuple(dataFiles.get(fileId), fileOff);
        }
      }
      blockNo = Util.byte2int(block, 0);
      if (blockNo == 0)
        return null;
    }
  }

  // get all order, entry size 5
  public List<Tuple> getAll(int blockNo, boolean buyer) throws Exception {
    List<Tuple> list = new ArrayList<>();
    byte[] block = new byte[BLOCK_SIZE];
    while (true) {
      synchronized (fd) {
        fd.seek(((long) blockNo) * BLOCK_SIZE);
        fd.read(block);
      }
      int size = Util.byte2short(block, 4);
      if (size == 0xffff) {
        long off = Util.byte2long(block, 6);
        return getFromDat(off, buyer);
      }
      if (size == 0) size = 6;
      for (int off = 6; off + 5 <= size; off += 5) {
        int fileId = block[off] & 0xff;
        long fileOff = Util.byte4ToLong(block, off + 1);
        Tuple tuple = new Tuple(dataFiles.get(fileId), fileOff);
        tuple.setRecord();
        list.add(tuple);
      }
      blockNo = Util.byte2int(block, 0);
      if (blockNo == 0)
        return list;
    }
  }

  // get all order from b2o.dat or g2o.dat
  private List<Tuple> getFromDat(long off, boolean buyer) throws Exception {
    RandomAccessFile fd;
    String filename;
    if (buyer) {
      fd = FdMap.b2odat;
      filename = FdMap.b2odatFilename;
    } else {
      //System.out.println("readgood" + off);
      fd = FdMap.g2odat;
      filename = FdMap.g2odatFilename;
    }
    byte[] buf = new byte[4096];
    synchronized (fd) {
      fd.seek(off);
      fd.read(buf);
    }
    int count = Util.byte2int(buf, 0);
    int bufOff = 4;
    List<Tuple> tupleList = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      long tupleOff = Util.byte2long(buf, bufOff);
      bufOff += 8;
      tupleList.add(new Tuple(filename, tupleOff));
    }
    return tupleList;
  }

  public void saveBuyerAll(List<BuyerResult> resultList, int blockNo) throws Exception {
    int size = resultList.size();
    RandomAccessFile bfd = FdMap.b2odat;
    byte[] buf = new byte[8];
    long fileLen;
    synchronized (bfd) {
      fileLen = bfd.length();
      Util.int2byte(size, buf, 0);
      bfd.seek(fileLen);
      bfd.write(buf, 0, 4);  // size
      long tupleOff = fileLen + 4 + 8 * size;
      // write head: size, off, off...
      for (BuyerResult result : resultList) {
        Util.long2byte(tupleOff, buf, 0);
        bfd.write(buf, 0, 8);  // off
        Tuple orderTuple = result.orderTuple;
        Tuple goodTuple = result.goodTuple;
        tupleOff += orderTuple.getTupleLen() + goodTuple.getTupleLen() + 2;
      }
      for (BuyerResult result : resultList) {
        Tuple orderTuple = result.orderTuple;
        Tuple goodTuple = result.goodTuple;

        List<byte[]> tupleContent = orderTuple.getTupleContent();
        int tupleLen = orderTuple.getTupleLen();
        int startOff = orderTuple.getTupleStartOff();
        int blockNum = tupleContent.size();
        if (blockNum == 1) {
          bfd.write(tupleContent.get(0), startOff, tupleLen);
        } else {
          bfd.write(tupleContent.get(0), startOff, 4096 - startOff);
          for (int i = 1; i < blockNum; i++)
            bfd.write(tupleContent.get(i));
        }
        bfd.write('\t');

        tupleContent = goodTuple.getTupleContent();
        tupleLen = goodTuple.getTupleLen();
        startOff = goodTuple.getTupleStartOff();
        blockNum = tupleContent.size();
        if (blockNum == 1) {
          bfd.write(tupleContent.get(0), startOff, tupleLen);
        } else {
          bfd.write(tupleContent.get(0), startOff, 4096 - startOff);
          for (int i = 1; i < blockNum; i++)
            bfd.write(tupleContent.get(i));
        }
        bfd.write('\n');
      }
    }
    Util.short2byte(0xffff, buf, 0);
    synchronized (fd) {
      fd.seek(((long) blockNo) * BLOCK_SIZE + 4);
      fd.write(buf, 0, 2);
      Util.long2byte(fileLen, buf, 0);
      fd.write(buf, 0, 8);
    }
  }

  public void saveGoodAll(List<GoodResult> resultList, int blockNo) throws Exception {
    int size = resultList.size();
    RandomAccessFile gfd = FdMap.g2odat;
    byte[] buf = new byte[8];
    long fileLen;
    synchronized (gfd) {
      fileLen = gfd.length();
      Util.int2byte(size, buf, 0);
      gfd.seek(fileLen);
      gfd.write(buf, 0, 4);  // size
      long tupleOff = fileLen + 4 + 8 * size;
      // write head: size, off, off...
      for (GoodResult result : resultList) {
        Util.long2byte(tupleOff, buf, 0);
        gfd.write(buf, 0, 8);  // off
        Tuple orderTuple = result.orderTuple;
        Tuple buyerTuple = result.buyerTuple;
        tupleOff += orderTuple.getTupleLen() + 1;
        if (buyerTuple != null)
          tupleOff += buyerTuple.getTupleLen() + 1;
      }
      for (GoodResult result : resultList) {
        Tuple orderTuple = result.orderTuple;
        Tuple buyerTuple = result.buyerTuple;

        List<byte[]> tupleContent = orderTuple.getTupleContent();
        int tupleLen = orderTuple.getTupleLen();
        int startOff = orderTuple.getTupleStartOff();
        int blockNum = tupleContent.size();
        if (blockNum == 1) {
          gfd.write(tupleContent.get(0), startOff, tupleLen);
        } else {
          gfd.write(tupleContent.get(0), startOff, 4096 - startOff);
          for (int i = 1; i < blockNum; i++)
            gfd.write(tupleContent.get(i));
        }
        if (buyerTuple != null) {
          gfd.write('\t');
          tupleContent = buyerTuple.getTupleContent();
          tupleLen = buyerTuple.getTupleLen();
          startOff = buyerTuple.getTupleStartOff();
          blockNum = tupleContent.size();
          if (blockNum == 1) {
            gfd.write(tupleContent.get(0), startOff, tupleLen);
          } else {
            gfd.write(tupleContent.get(0), startOff, 4096 - startOff);
            for (int i = 1; i < blockNum; i++)
              gfd.write(tupleContent.get(i));
          }
        }
        gfd.write('\n');
      }
    }
    Util.short2byte(0xffff, buf, 0);
    synchronized (fd) {
      fd.seek(((long) blockNo) * BLOCK_SIZE + 4);
      fd.write(buf, 0, 2);
      Util.long2byte(fileLen, buf, 0);
      fd.write(buf, 0, 8);
    }
    //System.out.println("savegood" + fileLen);
  }

  // 21 1 4 3
  // return true if find, false if not find, then create
  public boolean getBg(byte[] key, int keyLen, BgBytes bgBytes) {
    int size;
    byte[] block;
    int blockNo = Util.bytesHash(key, keyLen) % SIZE;
    while (true) {
      if (blockNo < SIZE)
        block = memory[blockNo];
      else
        block = memoryExt.get(blockNo - SIZE);
      size = Util.byte2short(block, 4);
      if (size == 0) size = 6;
      for (int off = 6; off + 29 <= size; off += 29) {
        if (keyLen == 21 && Util.bytesEqual(block, off, key, 0, 21) ||
            keyLen == 20 && block[off + 20] == 0 &&
                Util.bytesEqual(block, off, key, 0, 20)) {
          bgBytes.block = block;
          bgBytes.off = off + 21;
          return true;
        }
      }
      blockNo = Util.byte2int(block, 0);
      if (blockNo == 0) break;
    }
    if (size + 29 > BLOCK_SIZE) {
      blockNo = blockNum++;
      Util.int2byte(blockNo, block, 0);
      block = new byte[BLOCK_SIZE];
      memoryExt.add(block);
    }
    int nextPos = Util.byte2short(block, 4);
    if (nextPos == 0) nextPos = 6;
    // bg
    System.arraycopy(key, 0, block, nextPos, keyLen);
    Util.short2byte(nextPos + 29, block, 4);
    bgBytes.block = block;
    bgBytes.off = nextPos + 21;
    return false;
  }

  public Integer getBgId(byte[] key, int keyLen) {
    int size;
    byte[] block;
    int blockNo = Util.bytesHash(key, keyLen) % SIZE;
    while (true) {
      if (blockNo < SIZE)
        block = memory[blockNo];
      else
        block = memoryExt.get(blockNo - SIZE);
      size = Util.byte2short(block, 4);
      if (size == 0) size = 6;
      for (int off = 6; off + 29 <= size; off += 29) {
        if (keyLen == 21 && Util.bytesEqual(block, off, key, 0, 21) ||
            keyLen == 20 && block[off + 20] == 0 &&
                Util.bytesEqual(block, off, key, 0, 20)) {
          return Util.byte3Toint(block, off + 26);
        }
      }
      blockNo = Util.byte2int(block, 0);
      if (blockNo == 0) return null;
    }
  }

  public Tuple getBgTuple(byte[] key, int keyLen) {
    int size;
    byte[] block;
    int blockNo = Util.bytesHash(key, keyLen) % SIZE;
    while (true) {
      if (blockNo < SIZE)
        block = memory[blockNo];
      else
        block = memoryExt.get(blockNo - SIZE);
      size = Util.byte2short(block, 4);
      if (size == 0) size = 6;
      for (int off = 6; off + 29 <= size; off += 29) {
        if (keyLen == 21 && Util.bytesEqual(block, off, key, 0, 21) ||
            keyLen == 20 && block[off + 20] == 0 &&
                Util.bytesEqual(block, off, key, 0, 20)) {
          int fileId = block[off + 21] & 0xff;
          long fileOff = Util.byte4ToLong(block, off + 22);
          return new Tuple(dataFiles.get(fileId), fileOff);
        }
      }
      blockNo = Util.byte2int(block, 0);
      if (blockNo == 0) return null;
    }
  }

  public void writeFile() throws Exception {
    System.out.println(System.currentTimeMillis() + " [yfy] writeFile start");

    BufferedOutputStream bos = new BufferedOutputStream(
        new FileOutputStream(indexFile));
    for (int i = 0; i < SIZE; i++)
      bos.write(memory[i]);
    for (byte[] block : memoryExt)
      bos.write(block);
    bos.close();

    System.out.println("[yfy] size: " + SIZE + " extSize: " + memoryExt.size());
    System.out.println(System.currentTimeMillis() + " [yfy] writeFile end");

    memory = null;
    memoryExt = null;

    fd = new RandomAccessFile(indexFile, "rw");
  }

  public void printBgIndexSize() {
    System.out.println("[yfy] bg index size: " + SIZE + " extSize: " + memoryExt.size());
  }

  //    if (blockSize == 4096)
//      BIT = 12;
//    else if (blockSize == 2048)
//      BIT = 11;
//    else if (blockSize == 1024)
//      BIT = 10;
//    else if (blockSize == 512)
//      BIT = 9;

//  private static class Meta {
//    int next, size;
//    Meta nextMeta;
//  }

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
//    SIZE = blockNum = size;
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
//          byte[] blockNumsBytes = Util.int2byte(blockNum);
//          System.arraycopy(blockNumsBytes, 0, bucket, 0, 4);
//
//          cache.writeBlock(indexFile, blockNo, bucket);
//
//          bucket = new byte[BLOCK_SIZE];
//          System.arraycopy(Util.int2byte(8), 0, bucket, 4, 4);
//          nextPos = 8;
//
//          blockNo = blockNum;
//          blockNum++;
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
//        byte[] blockNumsBytes = Util.int2byte(blockNum);
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
//        blockNo = blockNum;
//        blockNum++;
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
//      byte[] blockNumsBytes = Util.int2byte(blockNum);
//      System.arraycopy(blockNumsBytes, 0, bucket, 0, 4);
//
//      cache.writeBlock(indexFile, blockNo, bucket);
//
//      bucket = new byte[BLOCK_SIZE];
//      System.arraycopy(Util.int2byte(8), 0, bucket, 4, 4);
//      nextPos = 8;
//
//      blockNo = blockNum;
//      blockNum++;
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
