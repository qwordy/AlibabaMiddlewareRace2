package com.alibaba.middleware.race;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by yfy on 7/27/16.
 * ReadBuffer
 */
public class ReadBuffer implements Runnable {

  private final static int SIZE = 1 << 24;

  private BlockingQueue<byte[]> queue;

  private byte[] buf;

  private int pos;

  private BufferedInputStream bis;

  public ReadBuffer(String filename) throws Exception {
    queue = new ArrayBlockingQueue<>(1);
    bis = new BufferedInputStream(new FileInputStream(filename));
  }

  public int read() throws Exception {
    if (buf == null)
      buf = queue.take();
    if (buf.length == 0)
      return -1;
    if (pos >= buf.length) {
      buf = queue.take();
      pos = 0;
    }
    if (buf.length == 0)
      return -1;
    return buf[pos++];
  }

  @Override
  public void run() {
    try {
      while (true) {
        byte[] buf = new byte[SIZE];
        int n = bis.read(buf);
        if (n == -1)
          buf = new byte[0];
        else if (n < SIZE)
          buf = Arrays.copyOf(buf, n);
        queue.put(buf);
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
