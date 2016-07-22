package com.alibaba.middleware.race.result;

import com.alibaba.middleware.race.HashTable;
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
    int goodResultMapSize = goodResultMap.size();
    if (goodResultMap.containsKey("goodid"))
      goodResultMapSize--;

    this.keys = keys;
    int targetSize = 0;
    if (keys != null) {
      targetSize = keys.size();
      if (!keys.contains("orderid"))
        targetSize++;
    }

    resultMap = new HashMap<>();
    scan(orderTuple, resultMap);
    if (keys == null || resultMap.size() + goodResultMapSize < targetSize) {
      OrderSystem.KeyValue buyerKv = resultMap.get("buyerid");
      if (buyerKv != null) {
        Tuple buyerTuple = HashTable.buyerHashTable.get(buyerKv.valueAsString().getBytes());
        scan(buyerTuple, resultMap);
      }
    }
  }

  @Override
  public OrderSystem.KeyValue get(String key) {
    if (key.equals("orderid")) {
      if (keys.contains(key))
        return resultMap.get(key);
      return null;
    }

    OrderSystem.KeyValue kv = resultMap.get(key);
    if (kv != null)
      return kv;

    kv = goodResultMap.get(key);
    return kv;
  }

  @Override
  public OrderSystem.KeyValue[] getAll() {
    if (keys == null) {
      int len = resultMap.size() + goodResultMap.size() - 1;
      OrderSystem.KeyValue[] kvArray = new OrderSystem.KeyValue[len];
      int count = 0;
      for (OrderSystem.KeyValue kv : resultMap.values())
        kvArray[++count] = kv;
      goodResultMap.remove("goodid");
      for (OrderSystem.KeyValue kv : goodResultMap.values())
        kvArray[++count] = kv;
      return kvArray;
    }

    OrderSystem.KeyValue[] kvArray = new OrderSystem.KeyValue[keys.size()];
    int count = 0;
    for (OrderSystem.KeyValue kv : resultMap.values())
      if (!kv.key().equals("orderid") || keys.contains("orderid"))
        kvArray[++count] = kv;
    goodResultMap.remove("goodid");
    for (OrderSystem.KeyValue kv : goodResultMap.values())
      kvArray[++count] = kv;
    return kvArray;
  }

  @Override
  public long orderId() {
    try {
      return resultMap.get("orderid").valueAsLong();
    } catch (Exception e) {
      e.printStackTrace();
      return 0;
    }
  }

  @Override
  protected boolean needKey(byte[] key, int keyLen) {
    return keys == null ||
        keys.contains(new String(key, 0, keyLen)) ||
        new String(key, 0, keyLen).equals("orderid");
  }
}
