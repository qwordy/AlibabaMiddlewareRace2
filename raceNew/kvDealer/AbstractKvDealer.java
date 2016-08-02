package com.alibaba.middleware.race.kvDealer;

/**
 * Created by yfy on 7/15/16.
 * AbstractKvDealer
 */
public abstract class AbstractKvDealer implements IKvDealer {

  public final static byte[] orderidBytes =
      new byte[]{'o', 'r', 'd', 'e', 'r', 'i', 'd'};

  public final static byte[] goodidBytes =
      new byte[]{'g', 'o', 'o', 'd', 'i', 'd'};

  public final static byte[] buyeridBytes =
      new byte[]{'b', 'u', 'y', 'e', 'r', 'i', 'd'};

  public final static byte[] createtimeBytes =
      new byte[]{'c', 'r', 'e', 'a', 't', 'e', 't', 'i', 'm', 'e'};

  protected int fileId;

  public void setFileId(int fileId) {
    this.fileId = fileId;
  }

  protected boolean keyMatch(byte[] key, int keyLen, byte[] expectKey) {
    if (keyLen != expectKey.length)
      return false;
    for (int i = 0; i < keyLen; i++)
      if (key[i] != expectKey[i])
        return false;
    return true;
  }
}
