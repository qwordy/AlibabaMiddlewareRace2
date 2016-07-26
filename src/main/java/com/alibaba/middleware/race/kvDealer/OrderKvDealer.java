package com.alibaba.middleware.race.kvDealer;

import com.alibaba.middleware.race.Util;
import com.alibaba.middleware.race.index.BgIndex;
import com.alibaba.middleware.race.index.OrderIndex;

import java.util.Arrays;

/**
 * Created by yfy on 7/15/16.
 * OrderKvDealer
 */
public class OrderKvDealer extends AbstractKvDealer {

  private OrderIndex orderIndex;

  private BgIndex buyerIndex, goodIndex;

  private int keyCount;

  private long curOffset;

  private byte[] orderidValue, buyeridValue, goodidValue, createtimeValue;

  // for test
  private int count;

  public OrderKvDealer(OrderIndex orderIndex, BgIndex buyerIndex, BgIndex goodIndex) {
    this.orderIndex = orderIndex;
    this.buyerIndex = buyerIndex;
    this.goodIndex = goodIndex;
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
      //orderIndex.add(orderidValue, fileId, curOffset);
      //buyerIndex.addOrder(buyeridValue, fileId, curOffset, createtimeValue);
      //goodIndex.addOrder(goodidValue, fileId, curOffset, orderidValue);
      return 2;
    }
    return 0;
  }
}
