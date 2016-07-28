package com.alibaba.middleware.race;

import com.alibaba.middleware.race.cache.ConcurrentCache;

/**
 * Created by yfy on 7/15/16.
 * Tuple in data file.
 */
public class Tuple {

  private String file;

  private long offset, data, pos;

  private byte[] buf;

  private ConcurrentCache cache;

  // whether buf has more bytes to read
  private boolean valid;

  private final int BLOCK_SIZE = 4096;
  private final int BIT = 12;
  private final int MASK = 0xfff;

  public Tuple(String file, long offset) {
    this.file = file;
    this.offset = offset;
    //this.data = data;
    pos = offset;  // current pos
    cache = ConcurrentCache.getInstance();
    valid = false;
  }

  /**
   * @return next byte, -1 when end
   */
  public int next() throws Exception {
    if (!valid) {
      buf = cache.readBlock(file, (int) (pos >>> BIT));
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

  public long getData() {
    return data;
  }

}
