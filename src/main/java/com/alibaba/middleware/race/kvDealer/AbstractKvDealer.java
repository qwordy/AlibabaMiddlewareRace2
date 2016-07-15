package com.alibaba.middleware.race.kvDealer;

/**
 * Created by yfy on 7/15/16.
 * AbstractKvDealer
 */
public abstract class AbstractKvDealer implements IKvDealer {

  private final static byte[] orderidBytes =
      new byte[]{'o', 'r', 'd', 'e', 'r', 'i', 'd'};

  private final static byte[] goodidBytes =
      new byte[]{'g', 'o', 'o', 'd', 'i', 'd'};

  private boolean keyMatch(byte[] key, int keyLen, byte[] expectKey) {
    if (keyLen != expectKey.length)
      return false;
    for (int i = 0; i < keyLen; i++)
      if (key[i] != expectKey[i])
        return false;
    return true;
  }
}
