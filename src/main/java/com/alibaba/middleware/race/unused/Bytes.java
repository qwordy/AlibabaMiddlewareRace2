package com.alibaba.middleware.race.unused;

/**
 * Created by yfy on 7/24/16.
 * Bytes
 */
public class Bytes {

  public byte[] bb;

  public Bytes(byte[] bb) {
    this.bb = bb;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj instanceof Bytes) {
      Bytes bytes = (Bytes) obj;
      int len = bb.length;
      if (len == bytes.bb.length) {
        for (int i = 0; i < len; i++)
          if (bb[i] != bytes.bb[i])
            return false;
        return true;
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h = 0;
    for (byte b : bb) {
      h = 31 * h + b;
    }
    return h;
  }
}
