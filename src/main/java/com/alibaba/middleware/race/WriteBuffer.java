package com.alibaba.middleware.race;

import java.io.RandomAccessFile;

/**
 * Created by yfy on 7/24/16.
 * WriteBuffer
 */
public class WriteBuffer implements Runnable {

  private int bufNum, bufSize, fdMoveBit;

  private byte[][] bufs;

  private int[] bufLens;

  private RandomAccessFile fd;

  public WriteBuffer(int bufNum, int bufSize, int fdNum, int fdMoveBit) {
    this.bufNum = bufNum;
    this.bufSize = bufSize;
    this.fdMoveBit = fdMoveBit;
    bufs = new byte[bufNum][];
    bufLens = new int[bufNum];
    fds = new RandomAccessFile[fdNum];
    for (int i = 0; i < bufNum; i++) {
      bufs[i] = new byte[bufSize];
    }

  }

  public void addFd(int id, RandomAccessFile fd) {
    fds[id] = fd;
  }

  public void add(int id, byte[] buf) throws Exception {
    synchronized (bufs[id]) {
      while (bufLens[id] + 14 > bufSize)
        bufs[id].wait();

    }
  }

  @Override
  public void run() {
    try {
      while (true) {
        for (int i = 0; i < bufNum; i++) {
          synchronized (bufs[i]) {
            RandomAccessFile fd = fds[i >> fdMoveBit];

          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
