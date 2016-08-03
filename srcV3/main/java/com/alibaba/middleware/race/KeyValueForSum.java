package com.alibaba.middleware.race;

/**
 * Created by yfy on 7/18/16.
 * KeyValueForSum
 */
public class KeyValueForSum implements OrderSystem.KeyValue{

  private String key;

  private long sumLong;

  private double sumDouble;

  public KeyValueForSum(String key, long sumLong, double sumDouble) {
    this.key = key;
    this.sumLong = sumLong;
    this.sumDouble = sumDouble;
  }

  @Override
  public String key() {
    return key;
  }

  @Override
  public String valueAsString() {
    return null;
  }

  @Override
  public long valueAsLong() throws OrderSystem.TypeException {
    return sumLong;
  }

  @Override
  public double valueAsDouble() throws OrderSystem.TypeException {
    return sumDouble;
  }

  @Override
  public boolean valueAsBoolean() throws OrderSystem.TypeException {
    return false;
  }
}
