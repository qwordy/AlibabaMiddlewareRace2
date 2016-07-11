package com.alibaba.middleware.race;

import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by yfy on 7/11/16.
 * Cache
 */
public class Cache {

  private final int BlockSize = 4096;

  private final int CacheSize = 1024;

  // blockId, block
  private Map<BlockId, byte[]> blockMap;

  // filename, fd
  private Map<String, RandomAccessFile> fileMap;

  public Cache() {
    blockMap = new LinkedHashMap<BlockId, byte[]>(
        (int) Math.ceil(CacheSize / 0.75f) + 1, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry eldest) {
        return size() > CacheSize;
      }
    };

    fileMap = new HashMap<>();
  }

  public boolean read(String filename, long start, int length, byte[] buf) {
    return false;
  }

  public boolean write(String filename, long start, int length, byte[] buf) {
    return false;
  }

  private boolean readBlock(BlockId blockId, byte[] buf) throws Exception {
    byte[] block = blockMap.get(blockId);
    if (block == null) {  // not in cache
      RandomAccessFile f = fileMap.get(blockId.filename);
      if (f == null)
        f = new RandomAccessFile(blockId.filename, "rw");
      //f.seek();

    } else {
      System.arraycopy(block, 0, buf, 0, BlockSize);
    }

    return false;
  }

  private boolean writeBlock(BlockId blockId, byte[] buf) {
    byte[] block = blockMap.get(blockId);
    if (block == null) {  // not in cache

    } else {

    }

    return false;
  }

}
