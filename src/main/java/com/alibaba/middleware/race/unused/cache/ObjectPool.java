package com.alibaba.middleware.race.unused.cache;

import java.util.Vector;

/**
 * Created by yfy on 7/22/16.
 * ObjectPool
 */
public class ObjectPool<E> {

  private Vector<E> vector;

  public ObjectPool(int capacity) {
    vector = new Vector<>(capacity, 100);
  }

  public E get() {
    return null;
  }

  public void put(E obj) {

  }

}
