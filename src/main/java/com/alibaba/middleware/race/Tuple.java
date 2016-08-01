package com.alibaba.middleware.race;

import com.alibaba.middleware.race.cache.ConcurrentCache;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by yfy on 7/15/16.
 * Tuple in data file.
 */
public class Tuple {

  private RandomAccessFile fd;

  private long offset, pos;

  private byte[] buf;

  // whether buf has more bytes to read
  private boolean valid;

  private boolean recordContent;

  // exclude \n
  private List<byte[]> tupleContent;

  // exclude \n
  private int tupleContentLen;

  public Tuple(String file, long offset) {
    fd = FdMap.get(file);
    this.offset = offset;
    pos = offset;  // current pos
    valid = false;
    buf = new byte[4096];
  }

  public void setRecordContent() {
    recordContent = true;
    tupleContent = new ArrayList<>();
  }

  public boolean isRecordContent() {
    return recordContent;
  }

  public List<byte[]> getTupleContent() {
    return tupleContent;
  }

  public int getTupleContentLen() {
    return tupleContentLen;
  }

  /**
   * @return next byte, -1 when end
   */
  public int next() throws Exception {
    int BUF_SIZE = 4096;
    int BIT = 12;
    int MASK = 0xfff;
    if (!valid) {
      synchronized (fd) {
        fd.seek((pos >>> BIT) << BIT);
        fd.read(buf);
      }
      valid = true;
    }
    int blockOff = (int) (pos & MASK);
    byte b = buf[blockOff];
    pos++;
    if (b == '\n' || b == '\r')
      return -1;
    if (blockOff == BUF_SIZE - 1)
      valid = false;
    return b;
  }

//  public void reset() {
//    pos = offset;
//    valid = false;
//  }

  //  public int next() throws Exception {
//    if (!valid) {
//      synchronized (fd) {
//        fd.seek(pos);
//        fd.read(buf);
//        blockOff = 0;
//      }
//      valid = true;
//    }
//    byte b = buf[blockOff];
//    pos++;
//    blockOff++;
//    if (b == '\n' || b == '\r') {
//      if (recordContent) {
//        tupleContent.add(Arrays.copyOf(buf, blockOff - 1));
//        tupleContentLen = (int) (pos - offset - 1);
//      }
//      return -1;
//    }
//    if (blockOff == 2048) {
//      if (recordContent)
//        tupleContent.add(Arrays.copyOf(buf, 2048));
//      valid = false;
//    }
//    return b;
//  }

}
