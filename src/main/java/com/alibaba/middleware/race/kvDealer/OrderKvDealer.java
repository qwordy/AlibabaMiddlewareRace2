package com.alibaba.middleware.race.kvDealer;

import com.alibaba.middleware.race.HashTable;
import com.alibaba.middleware.race.Util;

import java.util.Arrays;

/**
 * Created by yfy on 7/15/16.
 * OrderKvDealer
 */
public class OrderKvDealer extends AbstractKvDealer {

  private HashTable orderHashTable, buyer2OrderHashTable;

  private int keyCount;

  private long oldOffset;

  private byte[] buyeridValue, createtimeValue;

  public OrderKvDealer(HashTable orderHashTable, HashTable buyer2OrderHashTable) {
    this.orderHashTable = orderHashTable;
    this.buyer2OrderHashTable = buyer2OrderHashTable;
    keyCount = 0;
    oldOffset = -1;
  }

  @Override
  public int deal(byte[] key, int keyLen, byte[] value, int valueLen, long offset) throws Exception {
    if (keyMatch(key, keyLen, orderidBytes)) {
      keyCount = offset == oldOffset ? keyCount + 1 : 0;
      oldOffset = offset;

      long valueLong = Long.parseLong(new String(value, 0, valueLen));
      orderHashTable.add(Util.long2byte(valueLong), fileId, offset);

      if (keyCount == 4) return 2;
    } else if (keyMatch(key, keyLen, buyeridBytes)) {
      if (offset == oldOffset) {
        buyeridValue = Arrays.copyOf(value, valueLen);
        if (createtimeValue != null)
          buyer2OrderHashTable.addMulti(buyeridValue, fileId, offset, createtimeValue);
      } else {
        keyCount = 0;
        oldOffset = offset;
        buyeridValue = null;
        createtimeValue = null;
      }


    } else if (keyMatch(key, keyLen, createtimeBytes)) {

    } else if (keyMatch(key, keyLen, goodidBytes)) {

    }
    return 0;
  }
}
