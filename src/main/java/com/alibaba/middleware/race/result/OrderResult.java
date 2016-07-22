package com.alibaba.middleware.race.result;

import com.alibaba.middleware.race.OrderSystem;

/**
 * Created by yfy on 7/22/16.
 * OrderResult
 */
public class OrderResult extends AbstractResult implements OrderSystem.Result {

  public OrderResult() {

  }

  @Override
  public OrderSystem.KeyValue get(String key) {
    return null;
  }

  @Override
  public OrderSystem.KeyValue[] getAll() {
    return new OrderSystem.KeyValue[0];
  }

  @Override
  public long orderId() {
    return 0;
  }

  @Override
  protected boolean needKey(byte[] key, int keyLen) {
    return false;
  }
}
