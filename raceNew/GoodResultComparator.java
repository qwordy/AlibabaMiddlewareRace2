package com.alibaba.middleware.race;

import java.util.Comparator;

/**
 * Created by yfy on 7/30/16.
 * GoodResultComparator
 */
public class GoodResultComparator implements Comparator<OrderSystem.Result> {
  @Override
  public int compare(OrderSystem.Result o1, OrderSystem.Result o2) {
    long id1 = o1.orderId();
    long id2 = o2.orderId();
    if (id1 < id2)
      return -1;
    if (id1 > id2)
      return 1;
    return 0;
  }
}
