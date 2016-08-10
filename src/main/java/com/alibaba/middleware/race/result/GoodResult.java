package com.alibaba.middleware.race.result;

import com.alibaba.middleware.race.Database;
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

  private long orderid;

  private int goodResultMapSize, targetSize;

  public Tuple orderTuple, buyerTuple;

  public GoodResult(Tuple orderTuple, SimpleResult goodResult, Collection<String> keys)
      throws Exception {

    this.orderTuple = orderTuple;
    goodResultMap = goodResult.getResultMap();
    goodResultMapSize = goodResultMap.size();
    if (goodResultMap.containsKey("goodid"))
      goodResultMapSize--;

    this.keys = keys;
    if (keys != null) {
      targetSize = keys.size();
      if (!keys.contains("orderid"))
        targetSize++;
      if (!keys.contains("buyerid"))
        targetSize++;
    }

    resultMap = new HashMap<>();
    scan(orderTuple, resultMap);
  }

  // must be called after constructor
  public void phase2() throws Exception {
    if (keys == null || resultMap.size() + goodResultMapSize < targetSize) {
      OrderSystem.KeyValue buyerKv = resultMap.get("buyerid");
      if (buyerKv != null) {
        Tuple buyerTuple = Database.buyerIndex.getBg(buyerKv.valueAsString());
        if (orderTuple.isRecord()) {
          this.buyerTuple = buyerTuple;
          buyerTuple.setRecord();
        }
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
    }
    goodResultMap.remove("goodid");
  }

  @Override
  public OrderSystem.KeyValue get(String key) {
    OrderSystem.KeyValue kv = resultMap.get(key);
    if (kv != null)
      return kv;
    kv = goodResultMap.get(key);
    return kv;
  }

  @Override
  public OrderSystem.KeyValue[] getAll() {
    OrderSystem.KeyValue[] kvArray = new OrderSystem.KeyValue[keys.size()];
    int count = 0;
    for (OrderSystem.KeyValue kv : resultMap.values())
      kvArray[++count] = kv;
    for (OrderSystem.KeyValue kv : goodResultMap.values())
      kvArray[++count] = kv;
    return kvArray;
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
        keyStr.equals("buyerid");
//    return Util.keysContainKey(keys, key, keyLen) ||
//        Util.bytesEqual(key, 0, AbstractKvDealer.orderidBytes, 0, 7) ||
//        Util.bytesEqual(key, 0, AbstractKvDealer.buyeridBytes, 0, 7);
  }
}
