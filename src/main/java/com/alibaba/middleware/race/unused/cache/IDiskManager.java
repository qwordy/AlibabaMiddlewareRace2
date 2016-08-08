package com.alibaba.middleware.race.unused.cache;

/**
 * Created by yfy on 7/13/16.
 * IDiskManager
 */
public interface IDiskManager {

  /**
   * Read len bytes at position off in filename to buf.
   * Please guarantee buf's size >= length
   * @param filename
   * @param offset
   * @param length
   * @param buf
   * @throws Exception
   */
  public void read(String filename, long offset, int length, byte[] buf) throws Exception;

  /**
   * Write len bytes of buf to filename at position off.
   * Please guarantee buf's size >= length
   * @param filename
   * @param offset
   * @param length
   * @param buf
   * @throws Exception
   */
  public void write(String filename, long offset, int length, byte[] buf) throws Exception;

}
