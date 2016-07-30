package com.alibaba.middleware.race.kvDealer;

import com.alibaba.middleware.race.index.BgIndex;

/**
 * Created by yfy on 7/15/16.
 * GoodKvDealer
 */
public class GoodKvDealer extends AbstractKvDealer {

  private BgIndex goodIndex;

  //public static int count;

  public GoodKvDealer(BgIndex goodIndex) {
    this.goodIndex = goodIndex;
  }

  @Override
  public int deal(byte[] key, int keyLen, byte[] value, int valueLen, long offset) throws Exception {
    if (keyMatch(key, keyLen, goodidBytes)) {
      //count++;
      goodIndex.addBg(value, valueLen, fileId, offset);
      return 2;
    }
    return 0;
  }
}
