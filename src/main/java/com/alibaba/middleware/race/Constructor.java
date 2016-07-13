package com.alibaba.middleware.race;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by yfy on 7/13/16.
 * Constructor
 */
public class Constructor {

  public Constructor() {}

  public void readOrderFile(String filename, int id) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(filename));
    String line;
    long fileOff = 0;  // offset in file
    while ((line = br.readLine()) != null) {
      int lineOff = 0, sepPos, tabPos = 0;
      while (tabPos != -1) {
        sepPos = line.indexOf(':', lineOff);  // pos of :
        String key = line.substring(lineOff, sepPos);
        tabPos = line.indexOf('\t', lineOff);  // pos of tab
        String value;
        if (tabPos == -1)
          value = line.substring(sepPos + 1);
        else
          value = line.substring(sepPos + 1, tabPos);

        //System.out.println(key + "::" + value);

        lineOff = tabPos + 1;
      }
      //System.out.println();
    }
  }

}
