package com.alibaba.middleware.race;

import java.util.Comparator;

/**
 * Created by yfy on 7/18/16.
 * TupleCreatetimeComparator
 */
public class TupleCreatetimeComparator implements Comparator<Tuple> {

  @Override
  public int compare(Tuple t1, Tuple t2) {
    return (int) (t2.getExtra() - t1.getExtra());
  }
}
