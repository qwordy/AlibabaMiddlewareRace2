package com.alibaba.middleware.race;

import java.util.Collection;
import java.util.HashMap;

/**
 * Created by yfy on 7/14/16.
 * ResultImpl
 */
public class ResultImpl implements OrderSystem.Result {

  private Tuple orderTuple;

  // key, keyValue
  private HashMap<String, OrderSystem.KeyValue> map;

  public ResultImpl(Tuple orderTuple, Collection<String> keys) throws Exception {
    this.orderTuple = orderTuple;
    map = new HashMap<>();
    buildMap(keys);
  }

  private void buildMap(Collection<String> keys) throws Exception {
    int b;
    while ((b = orderTuple.next()) != -1) {

    }
  }

  @Override
  public OrderSystem.KeyValue get(String key) {
    return map.get(key);
  }

  @Override
  public OrderSystem.KeyValue[] getAll() {
    return (OrderSystem.KeyValue[]) map.values().toArray();
  }

  @Override
  public long orderId() {
    return 0;
  }

  public void printOrderTuple() throws Exception {
    int b;
    while ((b = orderTuple.next()) != -1)
      System.out.print((char) b);
    System.out.println();
  }
}
