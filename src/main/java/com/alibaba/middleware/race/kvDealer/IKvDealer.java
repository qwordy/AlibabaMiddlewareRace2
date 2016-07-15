package com.alibaba.middleware.race.kvDealer;

/**
 * Created by yfy on 7/15/16.
 * IKvDealer
 */
public interface IKvDealer {

  void deal(byte[] key, int keyLen, byte[] value, int valueLen);

}
