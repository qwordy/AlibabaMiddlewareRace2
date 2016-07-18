package com.alibaba.middleware.race;

import com.alibaba.middleware.race.cache.Cache;

/**
 * Created by yfy on 7/15/16.
 * Tuple in data file.
 */
public class Tuple {

  private String file;

  private long offset, extra, pos;

  private byte[] buf;

  private Cache cache;

  // whether buf has more bytes to read
  private boolean valid;

  private final int BLOCK_SIZE = 4096;
  private final int BIT = 12;
  private final int MASK = 0xfff;

  public Tuple(String file, long offset) {
    this(file, offset, 0);
  }

  public Tuple(String file, long offset, long extra) {
    this.file = file;
    this.offset = offset;
    this.extra = extra;
    pos = offset;  // current pos
    buf = new byte[BLOCK_SIZE];
    cache = Cache.getInstance();
    valid = false;
  }

  /**
   * @return next byte, -1 when end
   */
  public int next() throws Exception {
    if (!valid) {
      cache.readBlock(file, (int) (pos >>> BIT), buf);
      valid = true;
    }
    int blockOff = (int) (pos & MASK);
    byte b = buf[blockOff];
    pos++;
    if (b == '\n' || b == '\r')
      return -1;
    if (blockOff == BLOCK_SIZE - 1)
      valid = false;
    return b;
  }

  public void reset() {
    pos = offset;
    valid = false;
  }

  public long getExtra() {
    return extra;
  }

}
