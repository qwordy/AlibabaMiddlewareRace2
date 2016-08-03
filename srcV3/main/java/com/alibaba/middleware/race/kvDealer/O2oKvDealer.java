package com.alibaba.middleware.race.kvDealer;

import com.alibaba.middleware.race.Util;
import com.alibaba.middleware.race.index.OrderIndex;

/**
 * Created by yfy on 7/30/16.
 * O2oKvDealer
 */
public class O2oKvDealer extends AbstractKvDealer{

  private OrderIndex orderIndex;

  private byte[] orderidValue;

  public O2oKvDealer(OrderIndex orderIndex) {
    this.orderIndex = orderIndex;
    orderidValue = new byte[5];
  }

  @Override
  public int deal(byte[] key, int keyLen, byte[] value, int valueLen,
                  long offset) throws Exception {

    if (keyMatch(key, keyLen, orderidBytes)) {
      long orderidLong = parseLong(value, valueLen);
      Util.long2byte5(orderidLong, orderidValue, 0);
      orderIndex.add(orderidValue, fileId, offset);
      return 2;
    }
    return 0;
  }

  private long parseLong(byte[] b, int len) {
    long n = 0;
    for (int i = 0; i < len; i++)
      n = n * 10 + b[i] - '0';
    return n;
  }

}
