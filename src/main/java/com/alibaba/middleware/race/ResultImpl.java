package com.alibaba.middleware.race;

import java.util.*;

/**
 * Created by yfy on 7/14/16.
 * ResultImpl
 */
public class ResultImpl implements OrderSystem.Result {

  private Tuple orderTuple;

  // key, keyValue
  private HashMap<String, OrderSystem.KeyValue> resultMap;

  private Set<String> keySet;

  public ResultImpl(Tuple orderTuple, Collection<String> keys) throws Exception {
    this.orderTuple = orderTuple;
    if (keys != null) {
      keySet = new HashSet<>();
      keySet.add("orderid");
      for (String key : keys)
        keySet.add(key);
    }

    resultMap = new HashMap<>();
    buildMap();
  }

  private void buildMap() throws Exception {
    scan(orderTuple);
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
    return resultMap.get(key);
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
