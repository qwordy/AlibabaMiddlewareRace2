package com.alibaba.middleware.race;

import com.alibaba.middleware.race.cache.ConcurrentCache;

import java.io.RandomAccessFile;

/**
 * Created by yfy on 7/15/16.
 * Tuple in data file.
 */
public class Tuple {

  private String file;

  private long offset, pos, data;

  private byte[] buf;

  //private ConcurrentCache cache;

  // whether buf has more bytes to read
  private boolean valid;

  public Tuple(String file, long offset) {
    this.file = file;
    this.offset = offset;
    pos = offset;  // current pos
    valid = false;
    buf = new byte[4096];
  }

  /**
   * @return next byte, -1 when end
   */
  public int next() throws Exception {
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

  public long getData() {
    return data;
  }

}
