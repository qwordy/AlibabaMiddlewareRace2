package com.alibaba.middleware.race.kvDealer;

import com.alibaba.middleware.race.index.BgIndex;

/**
 * Created by yfy on 7/17/16.
 * BuyerKvDealer
 */
public class BuyerKvDealer extends AbstractKvDealer {

  private BgIndex buyerIndex;

  //public static int count;

  public BuyerKvDealer(BgIndex buyerIndex) {
    this.buyerIndex = buyerIndex;
  }

  @Override
  public int deal(byte[] key, int keyLen, byte[] value, int valueLen, long offset) throws Exception {
    if (keyMatch(key, keyLen, buyeridBytes)) {
      //count++;
      buyerIndex.addBg(value, valueLen, fileId, offset);
      return 2;
    }
    return 0;
  }
}
