package com.alibaba.middleware.race;

import com.alibaba.middleware.race.result.BuyerResult;

import java.util.Comparator;

/**
 * Created by yfy on 7/28/16.
 * BuyerResultComparator
 */
public class BuyerResultComparator implements Comparator<BuyerResult> {
  @Override
  public int compare(BuyerResult o1, BuyerResult o2) {
    long d = o1.getCreatetime() - o2.getCreatetime();
    if (d < 0)
      return 1;
    if (d > 0)
      return -1;
    return 0;
  }
}
