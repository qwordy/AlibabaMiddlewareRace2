package com.alibaba.middleware.race.unused.cache;

/**
 * Created by yfy on 7/22/16.
 * ICache
 */
public interface ICache {

  // copy block to buf
  void readBlock(String filename, int blockNo, byte[] buf) throws Exception;

  void writeBlock(String filename, int blockNo, byte[] buf) throws Exception;

}
