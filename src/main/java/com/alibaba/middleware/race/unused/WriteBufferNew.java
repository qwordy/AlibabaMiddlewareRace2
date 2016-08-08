package com.alibaba.middleware.race.unused;

import java.io.RandomAccessFile;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yfy on 7/27/16.
 * WriteBufferNew
 */
public class WriteBufferNew implements Runnable {

  // original number of block
  private int size;

  private int blockSize, maxCount;

  // current number of request
  private AtomicInteger count;

  private List<byte[]>[] bufs;

  // offset in block
  private int[] blockOffs;

  private Vector<List<byte[]>> extBufs;

  private Vector<Integer> extBlockOffs;

  private RandomAccessFile fd;

  private byte[] bigbuf;

  private boolean finish;

  final private byte[] lock = new byte[0];

  public WriteBufferNew(int size, int blockSize) {
    this.size = size;
    this.blockSize = blockSize;
    bufs = new LinkedList[size];
    for (int i = 0; i < size; i++)
      bufs[i] = new LinkedList<>();
    blockOffs = new int[size];
    extBufs = new Vector<>();
    extBlockOffs = new Vector<>();
    bigbuf = new byte[blockSize];
    count = new AtomicInteger();
    //maxCount = Config.eachIndexMaxCount;
  }

  public void setFd(RandomAccessFile fd) {
    this.fd = fd;
  }

  public void add(int id, byte[] buf) throws Exception {
    synchronized (lock) {
      while (count.get() >= maxCount)
        lock.wait();
    }
    count.incrementAndGet();
    if (id < size) {
      List<byte[]> list = bufs[id];
      synchronized (list) {
        list.add(Arrays.copyOf(buf, 14));
      }
    } else {
      id -= size;
      if (id < extBufs.size()) {
        List<byte[]> list = extBufs.get(id);
        synchronized (list) {
          list.add(Arrays.copyOf(buf, 14));
        }
      } else {
        List<byte[]> list = new LinkedList<>();
        list.add(Arrays.copyOf(buf, 14));
        extBufs.add(list);
        extBlockOffs.add(0);
      }
    }
  }

  public void finish() {
    finish = true;
  }

  @Override
  public void run() {
    try {
      Thread.sleep(3000);
      while (true) {
        System.out.println(count.get());
        boolean has = false;
        for (int i = 0; i < size; i++) {
          List<byte[]> list = bufs[i];
          synchronized (list) {
            int off = 0;
            for (byte[] buf : list) {
              has = true;
              System.arraycopy(buf, 0, bigbuf, off, 14);
              off += 14;
            }
            if (off > 0) {
              System.out.println(i);
              fd.seek((long) i * blockSize + blockOffs[i]);
              fd.write(bigbuf, 0, off);
              blockOffs[i] += off;
              count.addAndGet(-list.size());
              list.clear();
              synchronized (lock) {
                lock.notify();
              }
            }
          }
        }
        for (int i = 0; i < extBufs.size(); i++) {
          List<byte[]> list = extBufs.get(i);
          synchronized (list) {
            int off = 0;
            for (byte[] buf : list) {
              has = true;
              System.arraycopy(buf, 0, bigbuf, off, 14);
              off += 14;
            }
            int blockOff = extBlockOffs.get(i);
            fd.seek((long) (i + size) * blockSize + blockOff);
            if (off > 0) {
              System.out.println(size + i);
              fd.write(bigbuf, 0, off);
              extBlockOffs.set(i, blockOff + off);
              count.addAndGet(-list.size());
              list.clear();
              synchronized (lock) {
                lock.notify();
              }
            }
          }
        }
        if (!has && finish) {
          has = false;
          for (int i = 0; i < size; i++) {
            synchronized (bufs[i]) {
              if (!bufs[i].isEmpty()) {
                has = true;
                break;
              }
            }
          }
          if (has) continue;
          for (int i = 0; i < extBufs.size(); i++) {
            List<byte[]> list = extBufs.get(i);
            synchronized (list) {
              if (!list.isEmpty()) {
                has = true;
                break;
              }
            }
          }
          if (has) continue;
          return;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
