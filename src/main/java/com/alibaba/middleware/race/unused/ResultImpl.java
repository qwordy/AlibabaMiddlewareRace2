package com.alibaba.middleware.race.unused;

import com.alibaba.middleware.race.*;

import java.util.*;

/**
 * Created by yfy on 7/14/16.
 * ResultImpl
 */
public class ResultImpl implements OrderSystem.Result {

  private static final String orderidStr = "orderid";

  private static final String goodidStr = "goodid";

  private static final String buyeridStr = "buyerid";

  private Tuple orderTuple;

  // key, keyValue
  private HashMap<String, OrderSystem.KeyValue> resultMap;

  private Collection<String> keys;

  private Set<String> keySet;

  private boolean allScaned, orderScaned, goodScaned, buyerScaned;

  public ResultImpl(Tuple orderTuple, Collection<String> keys) throws Exception {
    this.orderTuple = orderTuple;
    this.keys = keys;
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
    try {
      Tuple goodTuple = Database.goodIndex.getBg(goodidKv.valueAsString());
      scan(goodTuple);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void scanBuyer() {
    OrderSystem.KeyValue buyeridKv = resultMap.get(buyeridStr);
    if (buyeridKv == null)
      return;
    try {
      Tuple buyerTuple = Database.buyerIndex.getBg(buyeridKv.valueAsString());
      scan(buyerTuple);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void scan(Tuple tuple) throws Exception {
    int b, keyLen = 0, valueLen = 0;
    // 0 for read key, 1 for read value
    int status = 0;
    byte[] key = new byte[256];
    byte[] value = new byte[105536];
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
    if (keys != null && !keys.contains(key))
      return null;

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

    if (!buyerScaned) {
      scanBuyer();
      buyerScaned = true;
      kv = resultMap.get(key);
      if (kv != null)
        return kv;
    }

    if (!goodScaned) {
      scanGood();
      allScaned = goodScaned = true;
      return resultMap.get(key);
    }

    return null;
  }

  @Override
  public OrderSystem.KeyValue[] getAll() {
    return (OrderSystem.KeyValue[]) resultMap.values().toArray();
  }

  @Override
  public long orderId() {
    if (!orderScaned) {
      scanOrder();
      orderScaned = true;
    }
    OrderSystem.KeyValue kv = resultMap.get(orderidStr);

    long id = 0;
    try {
      id = kv.valueAsLong();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return id;
  }

//  public void printOrderTuple() throws Exception {
//    int b;
//    orderTuple.reset();
//    while ((b = orderTuple.next()) != -1)
//      System.out.print((char) b);
//    System.out.println();
//  }
}
