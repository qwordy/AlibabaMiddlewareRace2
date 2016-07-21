package com.alibaba.middleware.race.result;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.Tuple;

import java.util.Collection;
import java.util.Set;

/**
 * Created by yfy on 7/21/16.
 * AbstractResult
 */
public abstract class AbstractResult implements OrderSystem.Result {

  private Collection<String> keys;

  private Set<String> keySet;

  public AbstractResult(Collection<String> keys) {

  }

}
