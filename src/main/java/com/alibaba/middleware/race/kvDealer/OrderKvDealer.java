package com.alibaba.middleware.race.kvDealer;

import com.alibaba.middleware.race.HashTable;
import com.alibaba.middleware.race.Util;

/**
 * Created by yfy on 7/15/16.
 * OrderKvDealer
 */
public class OrderKvDealer extends AbstractKvDealer {

  private HashTable orderHashTable;

  public OrderKvDealer(HashTable orderHashTable) {
    this.orderHashTable = orderHashTable;
  }

  @Override
  public int deal(byte[] key, int keyLen, byte[] value, int valueLen, long offset) throws Exception {
    if (keyMatch(key, keyLen, orderidBytes)) {
      long valueLong = Long.parseLong(new String(value, 0, valueLen));
      orderHashTable.add(Util.long2byte(valueLong), fileId, offset);
    }
  }
}
