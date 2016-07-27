package com.alibaba.middleware.race;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yfy on 7/27/16.
 * WriteBuffer0
 */
public class WriteBuffer0 implements Runnable {

  private AtomicInteger count;

  private List<byte[]>[] bufs;

  public WriteBuffer0(int blockNum, int blockSize) {
    bufs = new LinkedList[blockNum];

  }

  @Override
  public void run() {

  }
}
