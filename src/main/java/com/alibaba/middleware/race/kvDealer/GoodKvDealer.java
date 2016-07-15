package com.alibaba.middleware.race.kvDealer;

import com.alibaba.middleware.race.HashTable;

/**
 * Created by yfy on 7/15/16.
 * GoodKvDealer
 */
public class GoodKvDealer extends AbstractKvDealer {

  private HashTable table;

  public GoodKvDealer(HashTable table) {
    this.table = table;
  }

  @Override
  public int deal(byte[] key, int keyLen, byte[] value, int valueLen, long offset) throws Exception {
    if (keyMatch(key, keyLen, goodidBytes)) {
      byte[] vb = new byte[valueLen];
      System.arraycopy(value, 0, vb, 0, valueLen);
      table.add(vb, fileId, offset);
      return 2;
    }
    return 0;
  }
}
