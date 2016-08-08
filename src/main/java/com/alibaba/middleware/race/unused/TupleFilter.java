package com.alibaba.middleware.race.unused;

/**
 * Created by yfy on 7/18/16.
 * TupleFilter. order createtime
 */
public class TupleFilter {

  private long time0, time1;

  public TupleFilter(long time0, long time1) {
    this.time0 = time0;
    this.time1 = time1;
  }

  // time0 <= time <= time1
  public boolean test(long time) {
    return time0 <= time && time <= time1;
  }

}
