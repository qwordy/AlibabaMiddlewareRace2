package com.alibaba.middleware.race.result;

import com.alibaba.middleware.race.Database;
import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.Tuple;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by yfy on 7/22/16.
 * OrderResult
 */
public class OrderResult extends AbstractResult implements OrderSystem.Result {

  private Collection<String> keys;

  private Map<String, OrderSystem.KeyValue> resultMap;

  private long orderid;

  public OrderResult(Tuple orderTuple, Collection<String> keys)
      throws Exception {

    this.keys = keys;
    int targetSize = 0;
    if (keys != null) {
      targetSize = keys.size();
      if (!keys.contains("orderid"))
        targetSize++;
      if (!keys.contains("buyerid"))
        targetSize++;
      if (!keys.contains("goodid"))
        targetSize++;
    }

    resultMap = new HashMap<>();
    scan(orderTuple, resultMap);
    if (keys == null || resultMap.size() < targetSize) {
      OrderSystem.KeyValue goodKv = resultMap.get("goodid");
      if (goodKv != null) {
        Tuple goodTuple = Database.goodIndex.getBg(goodKv.valueAsString());
        scan(goodTuple, resultMap);
      }
    }
    if (keys == null || resultMap.size() < targetSize) {
      OrderSystem.KeyValue buyerKv = resultMap.get("buyerid");
      if (buyerKv != null) {
        Tuple buyerTuple = Database.buyerIndex.getBg(buyerKv.valueAsString());
        scan(buyerTuple, resultMap);
      }
    }

    try {
      orderid = resultMap.get("orderid").valueAsLong();
    } catch (Exception e) {}

    if (keys != null) {
      if (!keys.contains("orderid"))
        resultMap.remove("orderid");
      if (!keys.contains("buyerid"))
        resultMap.remove("buyerid");
      if (!keys.contains("goodid"))
        resultMap.remove("goodid");
    }
  }

  @Override
  public OrderSystem.KeyValue get(String key) {
    return resultMap.get(key);
  }

  @Override
  public OrderSystem.KeyValue[] getAll() {
    return (OrderSystem.KeyValue[]) resultMap.values().toArray();
  }

  @Override
  public long orderId() {
    return orderid;
  }

  @Override
  protected boolean needKey(byte[] key, int keyLen) {
    String keyStr = new String(key, 0, keyLen);
    return keys == null ||
        keys.contains(keyStr) ||
        keyStr.equals("orderid") ||
        keyStr.equals("buyerid") ||
        keyStr.equals("goodid");
  }
}
