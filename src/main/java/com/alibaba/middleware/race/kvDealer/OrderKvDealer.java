package com.alibaba.middleware.race.kvDealer;

import com.alibaba.middleware.race.HashTable;
import com.alibaba.middleware.race.Util;

import java.util.Arrays;

/**
 * Created by yfy on 7/15/16.
 * OrderKvDealer
 */
public class OrderKvDealer extends AbstractKvDealer {

  private HashTable orderHashTable, buyer2OrderHashTable, good2OrderHashTable;

  private int keyCount;

  private long oldOffset;

  private byte[] buyeridValue, createtimeValue;

  public OrderKvDealer(HashTable orderHashTable,
                       HashTable buyer2OrderHashTable,
                       HashTable good2OrderHashTable) {
    this.orderHashTable = orderHashTable;
    this.buyer2OrderHashTable = buyer2OrderHashTable;
    this.good2OrderHashTable = good2OrderHashTable;
    keyCount = 0;
    oldOffset = -1;
  }

  @Override
  public int deal(byte[] key, int keyLen, byte[] value, int valueLen, long offset) throws Exception {
    if (keyMatch(key, keyLen, orderidBytes)) {
      update(offset);

      long valueLong = Long.parseLong(new String(value, 0, valueLen));
      orderHashTable.add(Util.long2byte(valueLong), fileId, offset);

      if (keyCount == 4) return 2;

    } else if (keyMatch(key, keyLen, buyeridBytes)) {
      update(offset);

      buyeridValue = Arrays.copyOf(value, valueLen);
      if (createtimeValue != null)
        buyer2OrderHashTable.addMulti(buyeridValue, fileId, offset, createtimeValue);

      if (keyCount == 4) return 2;

    } else if (keyMatch(key, keyLen, createtimeBytes)) {
      update(offset);

      createtimeValue = Util.long2byte(Long.parseLong(new String(value, 0, valueLen)));
      if (buyeridValue != null)
        buyer2OrderHashTable.addMulti(buyeridValue, fileId, offset, createtimeValue);

      if (keyCount == 4) return 2;

    } else if (keyMatch(key, keyLen, goodidBytes)) {
      update(offset);
      if (keyCount == 4) return 2;
    }
    return 0;
  }

  private void update(long offset) {
    if (offset != oldOffset) {
      oldOffset = offset;
      keyCount = 0;
      buyeridValue = null;
      createtimeValue = null;
    }
    keyCount++;
  }
}
