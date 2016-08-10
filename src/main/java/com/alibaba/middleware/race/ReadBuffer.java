package com.alibaba.middleware.race;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by yfy on 7/27/16.
 * ReadBuffer
 */
public class ReadBuffer implements Runnable {

  private final static int SIZE = 1 << 22; //4m

  private byte[][] bufs;

  private int[] lens;

  private int pos, readPt, writePt;

  private BufferedInputStream bis;

  private final Lock lock = new ReentrantLock();

  // write condition
  private final Condition notFull = lock.newCondition();

  // read condition
  private final Condition notEmpty = lock.newCondition();

  public ReadBuffer(String filename) throws Exception {
    bis = new BufferedInputStream(new FileInputStream(filename));
    bufs = new byte[2][];
    bufs[0] = new byte[SIZE];
    bufs[1] = new byte[SIZE];
    lens = new int[2];
  }

  public void getBuf() throws Exception {
    lock.lock();
    try {
      while (readPt == writePt)
        notEmpty.await();
      readPt = (readPt + 1) & 1;
      pos = 0;
      notFull.signal();
    } finally {
      lock.unlock();
    }
  }

  public int read() throws Exception {
    if (pos >= lens[readPt]) {
      getBuf();
    }
    if (lens[readPt] == 0)
      return -1;
    //System.out.println(readPt + " " + pos);
    return bufs[readPt][pos++];
  }

  @Override
  public void run() {
    try {
      while (true) {
        int n;
        lock.lock();
        try {
          while (readPt != writePt)
            notFull.await();
          writePt = (writePt + 1) & 1;
          n = bis.read(bufs[writePt]);
          if (n == -1)
            lens[writePt] = 0;
          else
            lens[writePt] = n;
          notEmpty.signal();
        } finally {
          lock.unlock();
        }
        if (n == -1) {
          bis.close();
          return;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
