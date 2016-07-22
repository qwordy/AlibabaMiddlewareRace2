package com.alibaba.middleware.race.cache;

import com.alibaba.middleware.race.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

import java.io.RandomAccessFile;
import java.util.Map;

/**
 * Created by yfy on 7/22/16.
 * ConcurrentCache
 */
public class ConcurrentCache implements ICache {

  private final int BLOCK_SIZE = 4096;

  // 2 ^ BIT = BLOCK_SIZE
  private final int BIT = 12;

  private final int CACHE_SIZE = 300;

  private ConcurrentLinkedHashMap<BlockId, >

  // filename, fd
  private Map<String, RandomAccessFile> fileMap;

  private static ConcurrentCache cache;

  private ConcurrentCache() {

  }

  public static ConcurrentCache getInstance() {
    if (cache == null)
      cache = new ConcurrentCache();
    return cache;
  }

  @Override
  public void readBlock(String filename, int blockNo, byte[] buf) throws Exception {

  }

  @Override
  public void writeBlock(String filename, int blockNo, byte[] buf) throws Exception {

  }

  private static class BlockId {

    final String filename;

    // the no-th block in file
    final int no;

    BlockId(String filename, int no) {
      this.filename = filename;
      this.no = no;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof BlockId) {
        BlockId bi = (BlockId) obj;
        return filename.equals(bi.filename) && no == bi.no;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return filename.hashCode() + no;
    }
  }

}
