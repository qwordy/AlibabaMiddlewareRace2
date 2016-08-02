package com.alibaba.middleware.race.kvDealer;

import com.alibaba.middleware.race.index.BgIndex;

/**
 * Created by yfy on 7/31/16.
 * G2oKvDealer
 */
public class G2oKvDealer extends AbstractKvDealer {

  private BgIndex goodIndex;

  public G2oKvDealer(BgIndex goodIndex) {
    this.goodIndex = goodIndex;
  }

  @Override
  public int deal(byte[] key, int keyLen, byte[] value, int valueLen, long offset) throws Exception {
    if (keyMatch(key, keyLen, goodidBytes)) {
      goodIndex.addOrder(value, valueLen, fileId, offset);
      return 2;
    }
    return 0;
  }
}
