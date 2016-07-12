package com.alibaba.middleware.race;

import jdk.nashorn.internal.ir.Block;

import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by yfy on 7/11/16.
 * Cache
 */
public class Cache {

  private final int BLOCK_SIZE = 4096;

  // 2 ^ BIT = BLOCK_SIZE
  private final int BIT = 12;

  private final int MASK = 0xfff;

  private final int CACHE_SIZE = 1024;

  // blockId, block
  private Map<BlockId, Node> blockMap;

  Node head, tail;

  // filename, fd
  private Map<String, RandomAccessFile> fileMap;

  public Cache() {
    blockMap = new HashMap<>();
    fileMap = new HashMap<>();
    head = tail = null;
  }

  // read len bytes from position off in filename to buf
  // please guarantee buf's size >= len
  public boolean read(String filename, long off, long len, byte[] buf) {
    long beginBlockId = off >>> BIT;
    long endBlockId = (off + len) >>> BIT;
    if (beginBlockId == endBlockId) {
      // offset in the block
      long beginOff = off & MASK;
      byte[] block = readBlock();


    }

    return false;
  }

  // write len bytes of buf to filename from position off
  // please guarantee buf's size >= len
  public boolean write(String filename, long off, long len, byte[] buf) {
    return false;
  }

  // do not modify the return block
  private byte[] readBlock(BlockId blockId) throws Exception {
    Node node = blockMap.get(blockId);
    if (node == null) {  // not in cache
      // read from disk
      byte[] block = new byte[BLOCK_SIZE];
      RandomAccessFile f = getFd(blockId.filename);
      f.seek(blockId.id << BIT);
      f.read(block, 0, BLOCK_SIZE);

      node = new Node(blockId, block);

      // add a new node into cache
      if (blockMap.size() >= CACHE_SIZE) {  // remove the lru cache
        writeBlockToDisk(tail);
        remove(tail);
        blockMap.remove(tail.blockId);
      }
      addHead(node);
      blockMap.put(blockId, node);
    } else {  // in cache
      remove(node);
      addHead(node);
    }
    return node.block;
  }

  private void writeBlock(BlockId blockId, byte[] buf) throws Exception {
    Node node = blockMap.get(blockId);
    if (node == null) {  // not in cache
      // build a node
      byte[] block = new byte[BLOCK_SIZE];
      System.arraycopy(buf, 0, block, 0, BLOCK_SIZE);
      node = new Node(blockId, block);

      // add a new node into cache
      if (blockMap.size() >= CACHE_SIZE) {  // remove the lru cache
        writeBlockToDisk(tail);
        remove(tail);
        blockMap.remove(tail.blockId);
      }
      addHead(node);
      blockMap.put(blockId, node);
    } else {  // in cache
      System.arraycopy(buf, 0, node.block, 0, BLOCK_SIZE);
      remove(node);
      addHead(node);
    }
  }

  private RandomAccessFile getFd(String filename) throws Exception {
    RandomAccessFile f = fileMap.get(filename);
    if (f == null) {
      f = new RandomAccessFile(filename, "rwd");
      fileMap.put(filename, f);
    }
    return f;
  }

  private void writeBlockToDisk(Node node) throws Exception {
    RandomAccessFile f = getFd(node.blockId.filename);
    f.seek(node.blockId.id << BIT);
    f.write(node.block, 0, BLOCK_SIZE);
  }

  private void remove(Node node) {
    if (node.prev == null)
      head = node.next;
    else
      node.prev.next = node.next;

    if (node.next == null)
      tail = node.prev;
    else
      node.next.prev = node.prev;
  }

  private void addHead(Node node) {
    if (head == null)
      head = tail = node;
    else {
      node.prev = null;
      node.next = head;
      head.prev = node;
      head = node;
    }
  }

  /**
   * newest <--------> oldest
   * head  next  next  tail
   *    .   ->  . ->   .
   *        <-    <-
   *       prev  prev
   */
  private class Node {

    BlockId blockId;

    byte[] block;

    Node prev, next;

    Node(BlockId blockId, byte[] block) {
      this.blockId = blockId;
      this.block = block;
    }
  }

}
