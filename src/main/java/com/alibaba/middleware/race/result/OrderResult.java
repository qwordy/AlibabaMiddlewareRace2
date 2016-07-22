package com.alibaba.middleware.race.result;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.Tuple;

import java.util.Collection;

/**
 * Created by yfy on 7/22/16.
 * OrderResult
 */
public class OrderResult extends AbstractResult implements OrderSystem.Result {

  private Collection<String> keys;

  public OrderResult(Tuple orderTuple, Collection<String> keys) {
    this.keys = keys;
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
