package com.alibaba.middleware.race.result;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.Tuple;
import com.alibaba.middleware.race.Util;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by yfy on 7/21/16.
 * SimpleResult
 */
public class SimpleResult extends AbstractResult {

  private Collection<String> keys;

  private int targetSize;

  /**
   * @param tuple
   * @param keys null means all
   * @throws Exception
   */
  public SimpleResult(Tuple tuple, Collection<String> keys) throws Exception {
    resultMap = new HashMap<>();
    this.keys = keys;
    if (keys != null)
      targetSize = keys.size();
    scan(tuple);
  }

  public Map<String, OrderSystem.KeyValue> getResultMap() {
    return resultMap;
  }

  public OrderSystem.KeyValue get(String key) {
    return resultMap.get(key);
  }

  @Override
  protected boolean needKey(byte[] key, int keyLen) {
     return keys == null || keys.contains(new String(key, 0, keyLen));
//    return Util.keysContainKey(keys, key, keyLen);
  }

  @Override
  protected boolean done() {
    return keys != null && resultMap.size() >= targetSize;
  }
}
