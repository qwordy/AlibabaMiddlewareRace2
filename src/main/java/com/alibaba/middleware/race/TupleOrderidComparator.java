package com.alibaba.middleware.race;

import java.util.Comparator;

/**
 * Created by yfy on 7/18/16.
 * TupleOrderidComparator
 */
public class TupleOrderidComparator implements Comparator<Tuple> {

  @Override
  public int compare(Tuple t1, Tuple t2) {
    return (int) (t1.getExtra() - t2.getExtra());
  }
}
