package com.alibaba.middleware.race.kvDealer;

/**
 * Created by yfy on 7/15/16.
 * IKvDealer
 */
public interface IKvDealer {

  /**
   *
   * @param key
   * @param keyLen
   * @param value
   * @param valueLen
   * @param offset offset in file
   * @return 2 for find all keys and can skip the line
   * @throws Exception
   */
  int deal(byte[] key, int keyLen, byte[] value, int valueLen, long offset) throws Exception;

}
