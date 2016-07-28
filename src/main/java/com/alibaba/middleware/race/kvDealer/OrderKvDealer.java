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

  private int keyCount, buyeridLen, goodidLen;

  private long curOffset;

  private byte[] orderidValue, buyeridValue, goodidValue;

  // for test
  public static int count;

  public static long maxOid, minOid = Long.MAX_VALUE;

  public OrderKvDealer(OrderIndex orderIndex, BgIndex buyerIndex, BgIndex goodIndex) {
    this.orderIndex = orderIndex;
    this.buyerIndex = buyerIndex;
    this.goodIndex = goodIndex;
    keyCount = 0;
    curOffset = -1;
    orderidValue = new byte[5];
    buyeridValue = new byte[256];
    goodidValue = new byte[256];
  }

  @Override
  public int deal(byte[] key, int keyLen, byte[] value, int valueLen, long offset) throws Exception {
    if (keyMatch(key, keyLen, orderidBytes)) {
      update(offset);
      long orderidLong = Long.parseLong(new String(value, 0, valueLen));
      Util.long2byte5(orderidLong, orderidValue);
      if (orderidLong > maxOid) maxOid = orderidLong;
      if (orderidLong < minOid) minOid = orderidLong;
      return tryAdd();

    } else if (keyMatch(key, keyLen, buyeridBytes)) {
      update(offset);
      System.arraycopy(value, 0, buyeridValue, 0, valueLen);
      buyeridLen = valueLen;
      return tryAdd();

    } else if (keyMatch(key, keyLen, goodidBytes)) {
      update(offset);
      System.arraycopy(value, 0, goodidValue, 0, valueLen);
      goodidLen = valueLen;
      return tryAdd();
    }
    return 0;
  }

  private void update(long offset) {
    if (offset != curOffset) {
      curOffset = offset;
      keyCount = 0;
      //orderidValue = buyeridValue = goodidValue = createtimeValue = null;
    }
  }

  private int tryAdd() throws Exception {
    keyCount++;
    if (keyCount == 3) {
      count++;
//      orderIndex.add(orderidValue, fileId, curOffset);
//      buyerIndex.addOrder(buyeridValue, buyeridLen, fileId, curOffset);
//      goodIndex.addOrder(goodidValue, goodidLen, fileId, curOffset);
      return 2;
    }
    return 0;
  }

  //    } else if (keyMatch(key, keyLen, createtimeBytes)) {
//      update(offset);
//      long createtimeLong = Long.parseLong(new String(value, 0, valueLen));
//      createtimeValue = Util.long2byte(createtimeLong);
//      if (createtimeLong > maxTime) maxTime = createtimeLong;
//      if (createtimeLong < minTime) minTime = createtimeLong;
//      return tryAdd();
}
