package com.alibaba.middleware.race;

import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yfy on 7/29/16.
 * FdMap. Raw data read access
 */
public class FdMap {

  private static Map<String, RandomAccessFile> map;

  public static void init(List<String> files0, List<String> files1,
                          List<String> files2) throws Exception {

    map = new HashMap<>();
    for (String file : files0)
      map.put(file, new RandomAccessFile(file, "r"));
    for (String file : files1)
      map.put(file, new RandomAccessFile(file, "r"));
    for (String file : files2)
      map.put(file, new RandomAccessFile(file, "r"));
  }

  public static RandomAccessFile get(String filename) {
    return map.get(filename);
  }

}
