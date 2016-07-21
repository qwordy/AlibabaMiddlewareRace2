package com.alibaba.middleware.race.result;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.Tuple;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by yfy on 7/21/16.
 * GoodResult. queryOrdersBySaler
 */
public class GoodResult extends AbstractResult implements OrderSystem.Result {

  private Map<String, OrderSystem.KeyValue> goodResultMap, resultMap;

  private Collection<String> keys;

  public GoodResult(Tuple orderTuple, SimpleResult goodResult, Collection<String> keys)
      throws Exception {

    goodResultMap = goodResult.getResultMap();
    this.keys = keys;
    resultMap = new HashMap<>();
    scan(orderTuple, resultMap);
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
    return keys.contains(new String(key, 0, keyLen));
  }
}
