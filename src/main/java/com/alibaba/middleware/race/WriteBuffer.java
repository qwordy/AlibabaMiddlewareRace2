package com.alibaba.middleware.race;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yfy on 7/24/16.
 * WriteBuffer
 */
public class WriteBuffer implements Runnable {

  private int bufNum, bufMaxSize, blockSize;

  private List<Buffer> buffers;

  private RandomAccessFile fd;

  private boolean done;

  public WriteBuffer(int bufNum, int bufMaxSize, int blockSize) {
    this.bufNum = bufNum;
    this.bufMaxSize = bufMaxSize;
    this.blockSize = blockSize;
    buffers = new ArrayList<>(bufNum);
    for (int i = 0; i < bufNum; i++)
      buffers.add(new Buffer(bufMaxSize));
    //System.out.println("writebuffer add done.");
  }

  public void setFd(RandomAccessFile fd) {
    this.fd = fd;
  }

  // buf.length == 14
  public void add(int id, byte[] buf) throws Exception {
    Buffer buffer;
    if (id >= buffers.size()) {
      buffer = new Buffer(bufMaxSize);
      buffers.add(buffer);
    } else {
      buffer = buffers.get(id);
    }
    synchronized (buffer) {
      while (buffer.len + 14 > bufMaxSize)
        buffer.wait();
      System.arraycopy(buf, 0, buffer.buf, buffer.len, 14);
      buffer.len += 14;
    }
  }

  public void finish() {
    done = true;
  }

  @Override
  public void run() {
//    int a, b;
//    a = b = 0;
    try {
      Thread.sleep(5000);
      while (true) {
        boolean has = false;
        for (int i = 0; i < buffers.size(); i++) {
          Buffer buffer = buffers.get(i);
          synchronized (buffer) {
            if (buffer.len > 0) {
              //if (i < 1000000) a++; else b++;
              //System.out.println(i + " " + buffer.len + ' ' + a + ' ' + b);
              has = true;
              fd.seek((long) i * blockSize + buffer.offInBlock);
              fd.write(buffer.buf, 0, buffer.len);
              buffer.offInBlock += buffer.len;
              buffer.len = 0;
              buffer.notify();
            }
          }
        }
        if (!has && done) {
          System.out.println("[yfy] buffer size " + buffers.size());
          return;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static class Buffer {
    byte[] buf;
    int offInBlock, len;

    public Buffer(int size) {
      buf = new byte[size];
    }
  }
}
