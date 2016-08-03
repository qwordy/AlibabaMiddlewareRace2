package com.alibaba.middleware.race.kvDealer;

import com.alibaba.middleware.race.index.BgIndex;

/**
 * Created by yfy on 7/31/16.
 * B2oKvDealer
 */
public class B2oKvDealer extends AbstractKvDealer {

  private BgIndex buyerIndex;

  public B2oKvDealer(BgIndex buyerIndex) {
    this.buyerIndex = buyerIndex;
  }

  @Override
  public int deal(byte[] key, int keyLen, byte[] value, int valueLen, long offset) throws Exception {
    if (keyMatch(key, keyLen, buyeridBytes)) {
      buyerIndex.addOrder(value, valueLen, fileId, offset);
      return 2;
    }
    return 0;
  }
}
