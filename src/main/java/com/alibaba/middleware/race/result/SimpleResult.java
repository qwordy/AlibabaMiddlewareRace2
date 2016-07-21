package com.alibaba.middleware.race.result;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yfy on 7/21/16.
 * SimpleResult
 */
public class SimpleResult {

  private Map<String, OrderSystem.KeyValue> resultMap;

  public SimpleResult(Tuple tuple) {
    resultMap = new HashMap<>();
  }

}
