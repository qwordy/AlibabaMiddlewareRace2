package com.alibaba.middleware.race.result;

import com.alibaba.middleware.race.Database;
import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yfy on 7/21/16.
 * BuyerResult. queryOrdersByBuyer
 */
public class BuyerResult extends AbstractResult implements OrderSystem.Result {

  private Map<String, OrderSystem.KeyValue> buyerResultMap, resultMap;

  private long createtime;

  public Tuple orderTuple, goodTuple;

  public BuyerResult(Tuple orderTuple, SimpleResult buyerResult) throws Exception {
    this.orderTuple = orderTuple;
    buyerResultMap = buyerResult.getResultMap();
    resultMap = new HashMap<>();
    scan(orderTuple, resultMap);

    if (orderTuple.isRecord()) {
      OrderSystem.KeyValue goodKv = resultMap.get("goodid");
      if (goodKv != null) {
        Tuple goodTuple = Database.goodIndex.getBg(goodKv.valueAsString());
        this.goodTuple = goodTuple;
        goodTuple.setRecord();
        scan(goodTuple, resultMap);
      }
    }

    OrderSystem.KeyValue kv = resultMap.get("createtime");
    if (kv != null)
      createtime = kv.valueAsLong();
  }

  public long getCreatetime() {
    return createtime;
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
    int len = resultMap.size() + buyerResultMap.size() - 1;
    OrderSystem.KeyValue[] kvArray = new OrderSystem.KeyValue[len];
    int count = 0;
    for (OrderSystem.KeyValue kv : resultMap.values())
      kvArray[++count] = kv;
    buyerResultMap.remove("buyerid");
    for (OrderSystem.KeyValue kv : buyerResultMap.values())
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
    return true;
  }
}
