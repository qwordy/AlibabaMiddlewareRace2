package com.alibaba.middleware.race;

import java.io.RandomAccessFile;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by yfy on 7/24/16.
 * WriteBuffer
 */
public class WriteBuffer implements Runnable {

  private BlockingQueue<WriteRequest> queue;

  private RandomAccessFile fd;

  private String filename;

  public WriteBuffer(RandomAccessFile fd, String filename) {
    queue = new LinkedBlockingQueue<>(100000);
    this.fd = fd;
    this.filename = filename;
  }

  public void add(WriteRequest req) {
    try {
      queue.put(req);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void run() {
    while (true) {
      try {
        WriteRequest req = queue.take();
        //System.out.println(filename + " write buffer size " + queue.size());
        fd.seek(req.offset);
        fd.write(req.buf);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
