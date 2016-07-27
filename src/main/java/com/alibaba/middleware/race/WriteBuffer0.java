package com.alibaba.middleware.race;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yfy on 7/27/16.
 * WriteBuffer0
 */
public class WriteBuffer0 implements Runnable {

  // original number of block
  private int size;

  private int blockSize;

  private AtomicInteger count;

  private List<byte[]>[] bufs;

  // offset in block
  private int[] offs;

  private List<List<byte[]>> extBufs;

  private List<Integer> extOffs;

  private RandomAccessFile fd;

  private byte[] bigbuf;

  public WriteBuffer0(int size, int blockSize) {
    this.size = size;
    bufs = new LinkedList[size];
    offs = new int[size];
    extBufs = new ArrayList<>();
    extOffs = new ArrayList<>();
    bigbuf = new byte[4096];
  }

  public void add(int id, byte[] buf) {
    if (id < size) {
      List<byte[]> list = bufs[id];
      synchronized (list) {
        list.add(Arrays.copyOf(buf, 14));
      }
    } else {
      id -= size;
      if (id < extBufs.size()) {
        extBufs.get(id).add(Arrays.copyOf(buf, 14));
      } else {
        List<byte[]> list = new LinkedList<>();
        list.add(Arrays.copyOf(buf, 14));
        extBufs.add(list);
      }
    }
  }

  @Override
  public void run() {
    try {
      while (true) {
        for (int i = 0; i < size; i++) {
          List<byte[]> list = bufs[i];
          synchronized (list) {
            int off = 0;
            for (byte[] buf : list) {
              System.arraycopy(buf, 0, bigbuf, off, 14);
              off += 14;
            }
            fd.seek((long) i * blockSize + 0);
            fd.write(bigbuf, 0, off);
            list.clear();
          }
        }
        for (int i = 0; i < extBufs.size(); i++) {
          List<byte[]> list = extBufs.get(i);
          synchronized (list) {
            int off = 0;
            for (byte[] buf : list) {
              System.arraycopy(buf, 0, bigbuf, off, 14);
              off += 14;
            }
            fd.seek((long) (i + size) * blockSize + 0);
            fd.write(bigbuf, 0, off);
            list.clear();
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
