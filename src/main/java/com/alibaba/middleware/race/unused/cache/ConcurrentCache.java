package com.alibaba.middleware.race.unused.cache;

//import com.alibaba.middleware.race.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
//import com.alibaba.middleware.race.concurrentlinkedhashmap.EvictionListener;

/**
 * Created by yfy on 7/22/16.
 * ConcurrentCache
 */
public class ConcurrentCache {

//  private final int BLOCK_SIZE = 4096;
//
//  // 2 ^ BIT = BLOCK_SIZE
//  private final int BIT = 12;
//
//  private final int CACHE_SIZE = 300000;
//
//  private ConcurrentLinkedHashMap<BlockId, Node> blockMap;
//
//  // filename, fd
//  private final Map<String, RandomAccessFile> fileMap;
//
//  //private BlockIdPool blockIdPool;
//
//  private static ConcurrentCache cache;
//
//  private ConcurrentCache() {
//    EvictionListener<BlockId, Node> listener = new EvictionListener<BlockId, Node>() {
//      @Override
//      public void onEviction(BlockId key, Node value) {
//        try {
//          writeNodeToDisk(key, value);
//        } catch (Exception e) {
//          //e.printStackTrace();
//        }
//      }
//    };
//    blockMap = new ConcurrentLinkedHashMap.Builder<BlockId, Node>()
//        .maximumWeightedCapacity(CACHE_SIZE)
//        .listener(listener)
//        .build();
//    fileMap = new HashMap<>();
//    //blockIdPool = new BlockIdPool(CACHE_SIZE);
//  }
//
//  public static ConcurrentCache getInstance() {
//    if (cache == null)
//      cache = new ConcurrentCache();
//    return cache;
//  }
//
//  // copy to buf
//  public void readBlock(String filename, int blockNo, byte[] buf) throws Exception {
//    byte[] block = readBlock(filename, blockNo);
//    System.arraycopy(block, 0, buf, 0, BLOCK_SIZE);
//  }
//
//  // block itself
//  public byte[] readBlock(String filename, int blockNo) throws Exception {
//    BlockId blockId = new BlockId(filename, blockNo);
//    Node node = blockMap.get(blockId);
//    if (node == null) { // not in cache
//      //System.out.println("[yfy] miss " + filename + ' ' + blockNo);
//      synchronized (this) {
//        node = blockMap.get(blockId);
//        if (node == null) {
//          // read from disk
//          byte[] block = new byte[BLOCK_SIZE];
//          RandomAccessFile f = getFd(filename);
//          //synchronized (f) {
//          f.seek(((long) blockId.no) << BIT);
//          f.read(block, 0, BLOCK_SIZE);
//          //}
//
//          node = new Node(block);
//          blockMap.put(blockId, node);
//          return node.block;
//        }
//      }
//    }
//    //blockIdPool.put(blockId);
//    return node.block;
//  }
//
//  // buf should not be modified
//  public void writeBlock(String filename, int blockNo, byte[] buf) throws Exception {
//    BlockId blockId = new BlockId(filename, blockNo);
//    Node node = blockMap.get(blockId);
//    if (node == null) { // not in cache
//      node = new Node(buf);
//      node.modified = true;
//      blockMap.put(blockId, node);
//    } else { // in cache
//      //blockIdPool.put(blockId);
//      if (node.block != buf)
//        System.arraycopy(buf, 0, node.block, 0, BLOCK_SIZE);
//      node.modified = true;
//    }
//  }
//
//  public void addFd(String filename, boolean readOnly) throws Exception {
//    String mode = readOnly ? "r" : "rw";
//    fileMap.put(filename, new RandomAccessFile(filename, mode));
//  }
//
//  private RandomAccessFile getFd(String filename) throws Exception {
//    RandomAccessFile f = fileMap.get(filename);
//    if (f == null) {
//      synchronized (fileMap) {
//        f = fileMap.get(filename);
//        if (f == null) {
//          f = new RandomAccessFile(filename, "rw");
//          fileMap.put(filename, f);
//        }
//      }
//    }
//    return f;
//  }
//
//  private void writeNodeToDisk(BlockId blockId, Node node) throws Exception {
//    if (node.modified) {
//      RandomAccessFile f = getFd(blockId.filename);
//      synchronized (f) {
//        f.seek(((long) blockId.no) << BIT);
//        f.write(node.block, 0, BLOCK_SIZE);
//      }
//    }
//  }
//
//  private static class Node {
//
//    byte[] block;
//
//    boolean modified;
//
//    Node(byte[] block) {
//      this.block = block;
//      modified = false;
//    }
//  }
//
//  private static class NodePool {
//
//    Vector<Node> vector;
//
//  }
//
//  private static class BlockId {
//
//    String filename;
//
//    // the no-th block in file
//    int no;
//
//    BlockId(String filename, int no) {
//      this.filename = filename;
//      this.no = no;
//    }
//
//    @Override
//    public boolean equals(Object obj) {
//      if (obj instanceof BlockId) {
//        BlockId bi = (BlockId) obj;
//        return filename.equals(bi.filename) && no == bi.no;
//      }
//      return false;
//    }
//
//    @Override
//    public int hashCode() {
//      return filename.hashCode() + no;
//    }
//  }
//
//  private static class BlockIdPool {
//
//    List<BlockId> list;
//
//    public BlockIdPool(int size) {
//      list = new ArrayList<>(size);
//    }
//
//    public synchronized BlockId get(String filename, int no) {
//      if (list.isEmpty())
//        return new BlockId(filename, no);
//      BlockId blockId = list.remove(list.size() - 1);
//      blockId.filename = filename;
//      blockId.no = no;
//      return blockId;
//    }
//
//    public synchronized void put(BlockId blockId) {
//      list.add(blockId);
//    }
//
//  }

}
