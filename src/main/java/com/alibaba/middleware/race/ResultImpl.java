package com.alibaba.middleware.race;

import java.util.*;

/**
 * Created by yfy on 7/14/16.
 * ResultImpl
 */
public class ResultImpl implements OrderSystem.Result {

  private final String orderidStr = "orderid";

  private final String goodidStr = "goodid";

  private final String buyeridStr = "buyerid";

  private Tuple orderTuple;

  // key, keyValue
  private HashMap<String, OrderSystem.KeyValue> resultMap;

  private Set<String> keySet;

  private boolean allScaned, orderScaned, goodScaned, buyerScaned;

  public ResultImpl(Tuple orderTuple, Collection<String> keys) throws Exception {
    this.orderTuple = orderTuple;
    if (keys != null) {
      keySet = new HashSet<>();
      keySet.add(orderidStr);
      keySet.add(goodidStr);
      keySet.add(buyeridStr);
      for (String key : keys)
        keySet.add(key);
    }

    resultMap = new HashMap<>();
  }

  private void buildMap() throws Exception {
    scan(orderTuple);
  }

  private void scanOrder() {
    try {
      scan(orderTuple);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void scanGood() {
    OrderSystem.KeyValue goodidKv = resultMap.get(goodidStr);
    if (goodidKv == null)
      return;
    goodidKv.valueAsString().getBytes()
  }

  private void scan(Tuple tuple) throws Exception {
    int b, keyLen = 0, valueLen = 0;
    // 0 for read key, 1 for read value
    int status = 0;
    byte[] key = new byte[256];
    byte[] value = new byte[65536];
    while ((b = tuple.next()) != -1) {
      if (status == 0) {
        if (b == ':') {
          valueLen = 0;
          status = 1;
        } else {
          key[keyLen++] = (byte) b;
        }
      } else {  // 1
        if (b == '\t') {
          if (keyInKeySet(key, keyLen)) {
            String keyStr = new String(key, 0, keyLen);
            String valueStr = new String(value, 0, valueLen);
            resultMap.put(keyStr, new KeyValueImpl(keyStr, valueStr));
          }
          keyLen = 0;
          status = 0;
        } else {
          value[valueLen++] = (byte) b;
        }
      }
    }

    if (keyInKeySet(key, keyLen)) {
      String keyStr = new String(key, 0, keyLen);
      String valueStr = new String(value, 0, valueLen);
      resultMap.put(keyStr, new KeyValueImpl(keyStr, valueStr));
    }
  }

  private boolean keyInKeySet(byte[] key, int keyLen) {
    if (keySet == null)
      return true;
    for (String queryKey : keySet) {
      if (queryKey.length() == keyLen) {
        if (Util.bytesEqual(key, 0, queryKey.getBytes(), 0, keyLen))
          return true;
      }
    }
    return false;
  }

  @Override
  public OrderSystem.KeyValue get(String key) {
    if (allScaned)
      return resultMap.get(key);

    OrderSystem.KeyValue kv = resultMap.get(key);
    if (kv != null)
      return kv;

    if (!orderScaned) {
      scanOrder();
      orderScaned = true;
      kv = resultMap.get(key);
      if (kv != null)
        return kv;
    }

    if (!goodScaned) {
      try {
        scanGood();
        goodScaned = true;
      } catch (Exception e) {
        e.printStackTrace();
      }
      kv = resultMap.get(key);
      if (kv != null)
        return kv;
    }

    if (!buyerScaned) {
      try {

        allScaned = buyerScaned = true;
      } catch (Exception e) {
        e.printStackTrace();
      }
      return resultMap.get(key);
    }
  }

  @Override
  public OrderSystem.KeyValue[] getAll() {
    return (OrderSystem.KeyValue[]) resultMap.values().toArray();
  }

  @Override
  public long orderId() {
    long id = 0;
    try {
      id = resultMap.get("orderid").valueAsLong();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return id;
  }

  public void printOrderTuple() throws Exception {
    int b;
    orderTuple.reset();
    while ((b = orderTuple.next()) != -1)
      System.out.print((char) b);
    System.out.println();
  }
}
