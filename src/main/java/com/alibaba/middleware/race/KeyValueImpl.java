package com.alibaba.middleware.race;

/**
 * Created by yfy on 7/15/16.
 * KeyValueImpl
 */
public class KeyValueImpl implements OrderSystem.KeyValue {

  private String key, value;

  private byte[] valueBytes;

  public KeyValueImpl(String key, String value) {
    this.key = key;
    this.value = value;
  }

  public KeyValueImpl(byte[] key, int keyLen, byte[] value, int valueLen) {
    this.key = new String(key, 0, keyLen);
    valueBytes = new byte[valueLen];
    System.arraycopy(value, 0, valueBytes, 0, valueLen);
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
