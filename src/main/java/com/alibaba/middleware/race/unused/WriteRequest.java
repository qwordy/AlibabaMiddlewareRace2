package com.alibaba.middleware.race.unused;

import java.io.RandomAccessFile;
import java.util.Arrays;

/**
 * Created by yfy on 7/24/16.
 * WriteRequest
 */
public class WriteRequest {
  public byte[] buf;
  //public RandomAccessFile fd;
  public long offset;

  public WriteRequest() {}

  public WriteRequest(byte[] buf, long offset) {
    this.buf = Arrays.copyOf(buf, buf.length);
    //this.fd = fd;
    this.offset = offset;
  }
}
