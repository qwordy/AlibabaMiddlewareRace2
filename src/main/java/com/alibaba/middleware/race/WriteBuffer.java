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

  public WriteBuffer() {
    queue = new LinkedBlockingQueue<>(10000);
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
        if (req.buf == null)
          break;
        //System.out.println(filename + " write buffer size " + queue.size());
        req.fd.seek(req.offset);
        req.fd.write(req.buf);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
