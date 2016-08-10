package com.alibaba.middleware.race.result;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.Tuple;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by yfy on 7/21/16.
 * SimpleResult
 */
public class SimpleResult extends AbstractResult {

  private Map<String, OrderSystem.KeyValue> resultMap;

  private Collection<String> keys;

  /**
   * @param tuple
   * @param keys null means all
   * @throws Exception
   */
  public SimpleResult(Tuple tuple, Collection<String> keys) throws Exception {
    resultMap = new HashMap<>();
    this.keys = keys;
    if (tuple != null)
      scan(tuple, resultMap);
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
}
