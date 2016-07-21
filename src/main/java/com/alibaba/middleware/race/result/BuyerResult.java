package com.alibaba.middleware.race.result;

import com.alibaba.middleware.race.HashTable;
import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yfy on 7/21/16.
 * BuyerResult
 */
public class BuyerResult extends AbstractResult implements OrderSystem.Result {

  private Map<String, OrderSystem.KeyValue> buyerResultMap, resultMap;

  public BuyerResult(Tuple orderTuple, SimpleResult buyerResult) throws Exception {
    buyerResultMap = buyerResult.getResultMap();
    resultMap = new HashMap<>();
    scan(orderTuple, resultMap);

    OrderSystem.KeyValue goodKv = resultMap.get("goodid");
    if (goodKv != null) {
      Tuple goodTuple = HashTable.goodHashTable.get(goodKv.valueAsString().getBytes());
      scan(goodTuple, resultMap);
    }
  }

  @Override
  public OrderSystem.KeyValue get(String key) {
    OrderSystem.KeyValue kv = resultMap.get(key);
    if (kv != null)
      return kv;

    kv = buyerResultMap.get(key);
    return kv;
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
    return true;
  }
}
