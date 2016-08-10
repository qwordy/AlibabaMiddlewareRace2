package com.alibaba.middleware.race.unused;

import com.alibaba.middleware.race.Util;
import com.alibaba.middleware.race.index.BgIndex;
import com.alibaba.middleware.race.index.OrderIndex;
import com.alibaba.middleware.race.kvDealer.AbstractKvDealer;

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

//  public static long maxOid, minOid = Long.MAX_VALUE;
//
//  public static int maxBl, minBl = Integer.MAX_VALUE,
//      maxGl, minGl = Integer.MAX_VALUE;

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
  public int deal(byte[] key, int keyLen, byte[] value, int valueLen,
                  long offset) throws Exception {

    if (keyMatch(key, keyLen, orderidBytes)) {
      if (offset != curOffset) {
        curOffset = offset;
        keyCount = 0;
      }
      long orderidLong = parseLong(value, valueLen);
      Util.long2byte5(orderidLong, orderidValue, 0);
      //if (orderidLong > maxOid) maxOid = orderidLong;
      //if (orderidLong < minOid) minOid = orderidLong;
      return tryAdd();

    } else if (keyMatch(key, keyLen, buyeridBytes)) {
      if (offset != curOffset) {
        curOffset = offset;
        keyCount = 0;
      }
      System.arraycopy(value, 0, buyeridValue, 0, valueLen);
      buyeridLen = valueLen;
      //if (buyeridLen > maxBl) maxBl = buyeridLen;
      //if (buyeridLen < minBl) minBl = buyeridLen;
      return tryAdd();

    } else if (keyMatch(key, keyLen, goodidBytes)) {
      if (offset != curOffset) {
        curOffset = offset;
        keyCount = 0;
      }
      System.arraycopy(value, 0, goodidValue, 0, valueLen);
      goodidLen = valueLen;
      //if (goodidLen > maxGl) maxGl = goodidLen;
      //if (goodidLen < minGl) minGl = goodidLen;
      return tryAdd();
    }
    return 0;
  }

  private long parseLong(byte[] b, int len) {
    long n = 0;
    for (int i = 0; i < len; i++)
      n = n * 10 + b[i] - '0';
    return n;
  }

  private void update(long offset) {
    if (offset != curOffset) {
      curOffset = offset;
      keyCount = 0;
    }
  }

  private int tryAdd() throws Exception {
    keyCount++;
    if (keyCount == 3) {
      //count++;
      if (orderIndex != null) {
        orderIndex.add(orderidValue, fileId, curOffset);
      } else {
        buyerIndex.addOrder(buyeridValue, buyeridLen, fileId, curOffset);
        goodIndex.addOrder(goodidValue, goodidLen, fileId, curOffset);
      }
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
