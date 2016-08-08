package com.alibaba.middleware.race;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
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

  // whether record tuple content
  private boolean record;

  // exclude \n
  private List<byte[]> tupleContent;

  // exclude \n
  private int tupleLen;

  // start pos in first block in tupleContent
  private int tupleStartOff;

  public Tuple(String file, long offset) {
    fd = FdMap.get(file);
    this.offset = offset;
    pos = offset;  // current pos
    valid = false;
    buf = new byte[4096];
    tupleStartOff = (int) (pos & 0xfff);
  }

  public void setRecord() {
    record = true;
    tupleContent = new ArrayList<>();
  }

  public boolean isRecord() {
    return record;
  }

  public List<byte[]> getTupleContent() {
    return tupleContent;
  }

  public int getTupleLen() {
    return tupleLen;
  }

  public int getTupleStartOff() {
    return tupleStartOff;
  }

  /**
   * @return next byte, -1 when end
   */
  public int next() throws Exception {
    int BLOCK_SIZE = 4096;
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
    if (b == '\n' || b == '\r') {
      if (record) {
        tupleContent.add(Arrays.copyOf(buf, blockOff));
        tupleLen = (int) (pos - offset);
      }
      return -1;
    }
    if (blockOff == BLOCK_SIZE - 1) {
      if (record)
        tupleContent.add(Arrays.copyOf(buf, BLOCK_SIZE));
      valid = false;
    }
    pos++;
    return b;
  }

  public void reset() {
    pos = offset;
    valid = false;
  }

  public long getData() {
    return 0;
  }

}
