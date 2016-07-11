package com.alibaba.middleware.race;

/**
 * Created by yfy on 7/11/16.
 * Test
 */
public class Test {

  public static void main(String[] args) {
    move();
  }

  private static void move() {
    int a, b;
    long t1, t2;

    t1 = System.currentTimeMillis();
    a = 98739287;
    for (int i = 0; i < 1000000000; i++) {
      a %= 256;
    }
    System.out.println(a);
    t2 = System.currentTimeMillis();
    System.out.println(t2 - t1);

    t1 = System.currentTimeMillis();
    a = 98739287;
    for (int i = 0; i < 1000000000; i++) {
      a &= 0x000000ff;
    }
    System.out.println(a);
    t2 = System.currentTimeMillis();
    System.out.println(t2 - t1);
  }
}
