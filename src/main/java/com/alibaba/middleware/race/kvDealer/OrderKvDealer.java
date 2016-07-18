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

  private long curOffset;

  private byte[] orderidValue, buyeridValue, goodidValue, createtimeValue;

  public OrderKvDealer(HashTable orderHashTable,
                       HashTable buyer2OrderHashTable,
                       HashTable good2OrderHashTable) {
    this.orderHashTable = orderHashTable;
    this.buyer2OrderHashTable = buyer2OrderHashTable;
    this.good2OrderHashTable = good2OrderHashTable;
    keyCount = 0;
    curOffset = -1;
  }

  @Override
  public int deal(byte[] key, int keyLen, byte[] value, int valueLen, long offset) throws Exception {
    if (keyMatch(key, keyLen, orderidBytes)) {
      update(offset);
      orderidValue = Util.long2byte(Long.parseLong(new String(value, 0, valueLen)));
      return tryAdd();

    } else if (keyMatch(key, keyLen, buyeridBytes)) {
      update(offset);
      buyeridValue = Arrays.copyOf(value, valueLen);
      return tryAdd();

    } else if (keyMatch(key, keyLen, createtimeBytes)) {
      update(offset);
      createtimeValue = Util.long2byte(Long.parseLong(new String(value, 0, valueLen)));
      return tryAdd();

    } else if (keyMatch(key, keyLen, goodidBytes)) {
      update(offset);
      goodidValue = Arrays.copyOf(value, valueLen);
      return tryAdd();
    }
    return 0;
  }

  private void update(long offset) {
    if (offset != curOffset) {
      curOffset = offset;
      keyCount = 0;
      orderidValue = buyeridValue = goodidValue = createtimeValue = null;
    }
  }

  private int tryAdd() throws Exception {
    keyCount++;
    if (keyCount == 4) {
      orderHashTable.add(orderidValue, fileId, curOffset);
      buyer2OrderHashTable.addMulti(buyeridValue, fileId, curOffset, createtimeValue);
      good2OrderHashTable.addMulti(goodidValue, fileId, curOffset, orderidValue);
      return 2;
    }
    return 0;
  }
}
