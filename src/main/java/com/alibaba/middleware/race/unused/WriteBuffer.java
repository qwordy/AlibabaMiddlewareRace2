package com.alibaba.middleware.race.unused;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

/**
 * Created by yfy on 7/24/16.
 * WriteBuffer
 */
public class WriteBuffer implements Runnable {

  private int size, blockSize, blockBufSize;

  private Buffer[] buffers;

  private List<Buffer> extBuffers;

  private RandomAccessFile fd;

  private boolean done;

  public WriteBuffer(int size, int blockSize, int blockBufSize) {
    this.size = size;
    this.blockSize = blockSize;
    this.blockBufSize = blockBufSize;
    buffers = new Buffer[size];
    for (int i = 0; i < size; i++)
      buffers[i] = new Buffer(blockBufSize);
    extBuffers = new Vector<>();
    //System.out.println("writebuffer add done.");
  }

  public void setFd(RandomAccessFile fd) {
    this.fd = fd;
  }

  public void add(int id, byte[] buf) throws Exception {
    //System.out.println("add " + id);
    Buffer buffer;
    int entrySize = buf.length;
    if (id < size) {
      buffer = buffers[id];
      synchronized (buffer) {
        while (buffer.len + entrySize > blockBufSize)
          buffer.wait();
        System.arraycopy(buf, 0, buffer.buf, buffer.len, entrySize);
        buffer.len += entrySize;
      }
    } else {
      id -= size;
      if (id < extBuffers.size()) {
        buffer = extBuffers.get(id);
        synchronized (buffer) {
          while (buffer.len + entrySize > blockBufSize)
            buffer.wait();
          System.arraycopy(buf, 0, buffer.buf, buffer.len, entrySize);
          buffer.len += entrySize;
        }
      } else {
        buffer = new Buffer(blockBufSize);
        System.arraycopy(buf, 0, buffer.buf, buffer.len, entrySize);
        buffer.len += entrySize;
        extBuffers.add(buffer);
      }
    }
  }

  public void finish() {
    done = true;
  }

  @Override
  public void run() {
    try {
      while (true) {
        Thread.sleep(2000);
        System.out.println(System.currentTimeMillis() + " write start");
        long writeLen = 0;
        boolean has = false;
        for (int i = 0; i < size; i++) {
          Buffer buffer = buffers[i];
          synchronized (buffer) {
            if (buffer.len > 0) {
              //System.out.println(i);
              writeLen += buffer.len;
              has = true;
              fd.seek((long) i * blockSize + buffer.offInBlock);
              fd.write(buffer.buf, 0, buffer.len);
              buffer.offInBlock += buffer.len;
              buffer.len = 0;
              buffer.notify();
            }
          }
        }
        int extBuffersSize = extBuffers.size();
        for (int i = 0; i < extBuffersSize; i++) {
          Buffer buffer = extBuffers.get(i);
          synchronized (buffer) {
            if (buffer.len > 0) {
              //System.out.println(size + i);
              writeLen += buffer.len;
              has = true;
              fd.seek((long) (size + i) * blockSize + buffer.offInBlock);
              fd.write(buffer.buf, 0, buffer.len);
              buffer.offInBlock += buffer.len;
              buffer.len = 0;
              buffer.notify();
            }
          }
        }
        System.out.println(System.currentTimeMillis() +
            " write end " + writeLen);
        if (!has && done) {
          for (int i = 0; i < size; i++) {
            Buffer buffer = buffers[i];
            synchronized (buffer) {
              if (buffer.len > 0) {
                has = true; break;
              }
            }
          }
          if (has) continue;
          extBuffersSize = extBuffers.size();
          for (int i = 0; i < extBuffersSize; i++) {
            Buffer buffer = extBuffers.get(i);
            synchronized (buffer) {
              if (buffer.len > 0) {
                has = true; break;
              }
            }
          }
          if (has) continue;
          System.out.println("[yfy] buffer size " + (size + extBuffers.size()));
          buffers = null;
          extBuffers = null;
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
