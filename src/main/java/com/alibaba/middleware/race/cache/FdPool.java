package com.alibaba.middleware.race.cache;

import java.io.RandomAccessFile;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by yfy on 7/22/16.
 * FdPool
 */
public class FdPool {

  private LinkedBlockingQueue<RandomAccessFile> queue;

  public FdPool(String filename, String mode) throws Exception {
    int n = 8;
    queue = new LinkedBlockingQueue<>(n);
    for (int i = 0; i < n; i++)
      queue.add(new RandomAccessFile(filename, mode));
  }

  public RandomAccessFile get() {
    try {
      return queue.take();
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  public void put(RandomAccessFile f) {
    queue.offer(f);
  }

}
