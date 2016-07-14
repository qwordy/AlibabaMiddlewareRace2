package com.alibaba.middleware.race.cache;

/**
 * Created by yfy on 7/11/16.
 * BlockId
 */
public class BlockId {

  public final String filename;

  // the no-th block in file
  public final int no;

  public BlockId(String filename, int no) {
    this.filename = filename;
    this.no = no;
  }
}
