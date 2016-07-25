package com.alibaba.middleware.race;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by yfy on 7/24/16.
 * WriteBuffer
 */
public class WriteBuffer implements Runnable {

  private List<BlockingQueue<WriteRequest>> queueList;

  private List<RandomAccessFile> fdList;

  public WriteBuffer() {
    queueList = new ArrayList<>();
    fdList = new ArrayList<>();
  }

  public void addQueue(RandomAccessFile fd) throws Exception {
    queueList.add(new LinkedBlockingQueue<WriteRequest>(10000));
    fdList.add(fd);
  }

  public void add(int id, WriteRequest req) {
    try {
      queueList.get(id).put(req);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void run() {
    try {
      while (true) {
        for (int i = 0; i < queueList.size(); i++) {
          BlockingQueue<WriteRequest> queue = queueList.get(i);
          RandomAccessFile fd = fdList.get(i);
          //System.out.println("write buffer size " + queue.size());
          WriteRequest req = queue.poll();
          while (req != null) {
            if (req.buf == null)
              return;
            fd.seek(req.offset);
            fd.write(req.buf);
            req = queue.poll();
          }
        }
        //Thread.sleep(1);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
