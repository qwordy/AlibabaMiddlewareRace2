package com.alibaba.middleware.race.result;

import com.alibaba.middleware.race.KeyValueImpl;
import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.Tuple;

import java.util.Map;

/**
 * Created by yfy on 7/21/16.
 * AbstractResult
 */
public abstract class AbstractResult {

  private byte[] key = new byte[256];

  private byte[] value = new byte[100000];

  protected void scan(Tuple tuple, Map<String, OrderSystem.KeyValue> resultMap)
      throws Exception {

    int b, keyLen = 0, valueLen = 0;
    // 0 for read key, 1 for read value
    int status = 0;
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
          if (needKey(key, keyLen)) {
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

    if (needKey(key, keyLen)) {
      String keyStr = new String(key, 0, keyLen);
      String valueStr = new String(value, 0, valueLen);
      resultMap.put(keyStr, new KeyValueImpl(keyStr, valueStr));
    }
  }

  protected abstract boolean needKey(byte[] key, int keyLen);

}
