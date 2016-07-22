package com.alibaba.middleware.race.cache;

import com.alibaba.middleware.race.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.alibaba.middleware.race.concurrentlinkedhashmap.EvictionListener;

import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by yfy on 7/22/16.
 * ConcurrentCache
 */
public class ConcurrentCache implements ICache {

  private final int BLOCK_SIZE = 4096;

  // 2 ^ BIT = BLOCK_SIZE
  private final int BIT = 12;

  private final int CACHE_SIZE = 300000;

  private ConcurrentLinkedHashMap<BlockId, Node> blockMap;

  // filename, fd
  private final Map<String, RandomAccessFile> fileMap;

  private static ConcurrentCache cache;

  private ConcurrentCache() {
    EvictionListener<BlockId, Node> listener = new EvictionListener<BlockId, Node>() {
      @Override
      public void onEviction(BlockId key, Node value) {
        try {
          writeNodeToDisk(key, value);
        } catch (Exception e) {
          //e.printStackTrace();
        }
      }
    };
    blockMap = new ConcurrentLinkedHashMap.Builder<BlockId, Node>()
        .maximumWeightedCapacity(CACHE_SIZE)
        .listener(listener)
        .build();
    fileMap = new HashMap<>();
  }

  public static ConcurrentCache getInstance() {
    if (cache == null)
      cache = new ConcurrentCache();
    return cache;
  }

  @Override
  public synchronized void readBlock(String filename, int blockNo, byte[] buf) throws Exception {
    BlockId blockId = new BlockId(filename, blockNo);
    Node node = blockMap.get(blockId);
    if (node == null) { // not in cache
      // read from disk
      byte[] block = new byte[BLOCK_SIZE];
      RandomAccessFile f = getFd(filename);
      f.seek(((long) blockId.no) << BIT);
      f.read(block, 0, BLOCK_SIZE);

      node = new Node(block);
      blockMap.put(blockId, node);
    }
    System.arraycopy(node.block, 0, buf, 0, BLOCK_SIZE);
  }

  @Override
  public void writeBlock(String filename, int blockNo, byte[] buf) throws Exception {
    BlockId blockId = new BlockId(filename, blockNo);
    Node node = blockMap.get(blockId);
    if (node == null) { // not in cache
      byte[] block = new byte[BLOCK_SIZE];
      System.arraycopy(buf, 0, block, 0, BLOCK_SIZE);
      node = new Node(block);
      node.modified = true;
      blockMap.put(blockId, node);
    } else { // in cache
      System.arraycopy(buf, 0, node.block, 0, BLOCK_SIZE);
      node.modified = true;
    }
  }

  public void addFd(String filename, boolean readOnly) throws Exception {
    String mode = readOnly ? "r" : "rw";
    fileMap.put(filename, new RandomAccessFile(filename, mode));
  }

  private RandomAccessFile getFd(String filename) throws Exception {
    RandomAccessFile f = fileMap.get(filename);
    if (f == null) {
      synchronized (fileMap) {
        f = fileMap.get(filename);
        if (f == null) {
          f = new RandomAccessFile(filename, "rw");
          fileMap.put(filename, f);
        }
      }
    }
    return f;
  }

  private void writeNodeToDisk(BlockId blockId, Node node) throws Exception {
    if (node.modified) {
      RandomAccessFile f = getFd(blockId.filename);
      synchronized (f) {
        f.seek(((long) blockId.no) << BIT);
        f.write(node.block, 0, BLOCK_SIZE);
      }
    }
  }

  private static class Node {

    byte[] block;

    boolean modified;

    Node(byte[] block) {
      this.block = block;
      modified = false;
    }
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
