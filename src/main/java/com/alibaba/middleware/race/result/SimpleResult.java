package com.alibaba.middleware.race.result;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yfy on 7/21/16.
 * SimpleResult
 */
public class SimpleResult extends AbstractResult {

  private Map<String, OrderSystem.KeyValue> resultMap;

  public SimpleResult(Tuple tuple) throws Exception {
    resultMap = new HashMap<>();
    scan(tuple, resultMap);
  }

  public Map<String, OrderSystem.KeyValue> getResultMap() {
    return resultMap;
  }

  @Override
  protected boolean needKey(byte[] key, int keyLen) {
    return true;
  }
}
