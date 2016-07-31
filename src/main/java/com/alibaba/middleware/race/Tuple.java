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

  private String file;

  private RandomAccessFile fd;

  private long offset, pos;

  private int blockOff;

  private byte[] buf;

  // whether buf has more bytes to read
  private boolean valid;

  private boolean recordContent;

  private List<byte[]> tupleContent;

  // include \n
  private int tupleContentLen;

  public Tuple(String file, long offset) {
    this.file = file;
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

  public int next() throws Exception {
    if (!valid) {
      synchronized (fd) {
        fd.seek(pos);
        fd.read(buf);
        blockOff = 0;
      }
      valid = true;
    }
    byte b = buf[blockOff];
    pos++;
    blockOff++;
    if (b == '\n' || b == '\r') {
      if (recordContent) {
        tupleContent.add(Arrays.copyOf(buf, blockOff));
        tupleContentLen = (int) (pos - offset);
      }
      return -1;
    }
    if (blockOff == 4096) {
      if (recordContent)
        tupleContent.add(Arrays.copyOf(buf, 4096));
      valid = false;
    }
    return b;
  }

  /**
   * @return next byte, -1 when end
   */
  public int nextt() throws Exception {
    int BUF_SIZE = 4096;
    int BIT = 12;
    int MASK = 0xfff;
    if (!valid) {
      RandomAccessFile fd = FdMap.get(file);
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

  public void reset() {
    pos = offset;
    valid = false;
  }

}
