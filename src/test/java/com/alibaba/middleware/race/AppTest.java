package com.alibaba.middleware.race;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

import java.io.RandomAccessFile;
import java.util.Arrays;

/**
 * Created by yfy on 7/11/16.
 * Test
 */
public class AppTest {

  @Test
  public void testmidium() throws Exception {
    OrderSystem os = new OrderSystemImpl();
    os.construct(Arrays.asList("order_records.txt"), null, null, null);
    os.queryOrder(2982262, null);
    os.queryOrder(2999777, null);
    os.queryOrder(3007737, null);
  }

  @Test
  public void testsmall() throws Exception {
    OrderSystem os = new OrderSystemImpl();
    os.construct(Arrays.asList("rec.txt"), null, null, null);
    os.queryOrder(345, null);
    os.queryOrder(199, null);
  }

  @Test
  public void util() {
    assertEquals(Long.MAX_VALUE, Util.byte2long(Util.long2byte(Long.MAX_VALUE)));
    assertEquals(Long.MIN_VALUE, Util.byte2long(Util.long2byte(Long.MIN_VALUE)));
    assertEquals(-1, Util.byte2long(Util.long2byte(-1)));
    assertEquals(0, Util.byte2long(Util.long2byte(0)));

    assertEquals(Integer.MAX_VALUE, Util.byte2int(Util.int2byte(Integer.MAX_VALUE)));
    assertEquals(Integer.MIN_VALUE, Util.byte2int(Util.int2byte(Integer.MIN_VALUE)));
    assertEquals(-1, Util.byte2int(Util.int2byte(-1)));
    assertEquals(0, Util.byte2int(Util.int2byte(0)));
  }

  @Test
  public void move() {
    int a, b;
    long t1, t2;

    t1 = System.currentTimeMillis();
    a = 98739287;
    for (int i = 0; i < 1000000000; i++) {
      a >>= 10;
    }
    System.out.println(a);
    t2 = System.currentTimeMillis();
    System.out.println(t2 - t1);

    t1 = System.currentTimeMillis();
    a = 98739287;
    for (int i = 0; i < 1000000000; i++) {
      a >>>= 10;
    }
    System.out.println(a);
    t2 = System.currentTimeMillis();
    System.out.println(t2 - t1);
  }

  @Test
  public void t() {
    System.out.println((long)0xfffffff << 6);
    System.out.println((int) (0xfffffffffL >> 4));
  }

  @Test
  public void disk() {
    try {
      RandomAccessFile f = new RandomAccessFile("test", "rwd");
      byte[] buf = new byte[4096];

      Arrays.fill(buf, (byte) 5);

      System.out.println(System.currentTimeMillis());
      f.write(buf);
      System.out.println(System.currentTimeMillis());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
