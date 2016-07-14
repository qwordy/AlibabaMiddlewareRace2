package com.alibaba.middleware.race;

/**
 * Created by yfy on 7/14/16.
 * ResultImpl
 */
public class ResultImpl implements OrderSystem.Result {

  private byte[] orderBytes;

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
}
