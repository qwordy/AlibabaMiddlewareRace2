package com.alibaba.middleware.race;

import com.alibaba.middleware.race.result.BuyerResult;

import java.util.Comparator;

/**
 * Created by yfy on 7/28/16.
 * BuyerResultComparator
 */
public class BuyerResultComparator implements Comparator<OrderSystem.Result> {
  @Override
  public int compare(OrderSystem.Result o1, OrderSystem.Result o2) {

    long d = ((BuyerResult)o1).getCreatetime() -
        ((BuyerResult)o2).getCreatetime();
    if (d < 0)
      return 1;
    if (d > 0)
      return -1;
    return 0;
  }
}
