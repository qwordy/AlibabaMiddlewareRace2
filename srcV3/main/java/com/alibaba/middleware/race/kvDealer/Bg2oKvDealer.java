package com.alibaba.middleware.race.kvDealer;

import com.alibaba.middleware.race.index.BgIndex;

/**
 * Created by yfy on 7/30/16.
 * Bg2oKvDealer
 */
public class Bg2oKvDealer extends AbstractKvDealer {

  private BgIndex buyerIndex, goodIndex;

  private boolean readBuyer, readGood;

  public Bg2oKvDealer(BgIndex buyerIndex, BgIndex goodIndex) {
    this.buyerIndex = buyerIndex;
    this.goodIndex = goodIndex;
  }

  @Override
  public int deal(byte[] key, int keyLen, byte[] value, int valueLen, long offset) throws Exception {
    if (keyMatch(key, keyLen, buyeridBytes)) {
      buyerIndex.addOrder(value, valueLen, fileId, offset);
      if (readGood) {
        readGood = false;
        return 2;
      }
      readBuyer = true;
    } else if (keyMatch(key, keyLen, goodidBytes)) {
      goodIndex.addOrder(value, valueLen, fileId, offset);
      if (readBuyer) {
        readBuyer = false;
        return 2;
      }
      readGood = true;
    }
    return 0;
  }
}
