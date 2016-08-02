package com.alibaba.middleware.race;

/**
 * Created by yfy on 7/15/16.
 * KeyValueImpl
 */
public class KeyValueImpl implements OrderSystem.KeyValue {

  private String key, value;

  public KeyValueImpl(String key, String value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public String key() {
    return key;
  }

  @Override
  public String valueAsString() {
    return value;
  }

  @Override
  public long valueAsLong() throws OrderSystem.TypeException {
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new OrderSystem.TypeException();
    }
  }

  @Override
  public double valueAsDouble() throws OrderSystem.TypeException {
    try {
      return Double.parseDouble(value);
    } catch (NumberFormatException e) {
      throw new OrderSystem.TypeException();
    }
  }

  @Override
  public boolean valueAsBoolean() throws OrderSystem.TypeException {
    if (value.equals("true"))
      return true;
    if (value.equals("false"))
      return false;
    throw new OrderSystem.TypeException();
  }
}
