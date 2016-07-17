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
  public void testMidium() throws Exception {
    OrderSystem os = new OrderSystemImpl();
    os.construct(Arrays.asList("order_records.txt"),
        Arrays.asList("buyer_records.txt"),
        Arrays.asList("good_records.txt"), null);

    OrderSystem.Result result;

    result = os.queryOrder(3007847, null);
    assertEquals(3007847, result.orderId());
    assertEquals(true, result.get("done").valueAsBoolean());
    assertEquals(117, result.get("amount").valueAsLong());
    assertEquals("椰子节一路工程授权如何苏子河纯利润，奎松离别剑打扮网上开店慌张四",
        result.get("remark").valueAsString());
    assertEquals(8380.42, result.get("app_order_3334_0").valueAsDouble(), 1e-6);

    assertEquals("一些", result.get("good_name").valueAsString());
    assertEquals(42.9, result.get("price").valueAsDouble(), 1e-6);
    assertEquals(null, result.get("ggg"));
    assertEquals("tm_758d7a5f-c254-4bb8-9587-d211a4327814",
        result.get("salerid").valueAsString());

    assertEquals("376 55715168", result.get("contactphone").valueAsString());
    assertEquals("三寸不烂之舌", result.get("buyername").valueAsString());

    result = os.queryOrder(2982725, Arrays.asList("amount", "hehe", "offprice"));
    assertEquals(2982725, result.orderId());
    assertEquals(220, result.get("amount").valueAsLong());
    assertEquals(null, result.get("hehe"));
    assertEquals(null, result.get("buyerid"));
    assertEquals(null, result.get("yyyy"));

    assertEquals(6.71, result.get("offprice").valueAsDouble(), 1e-6);

    result = os.queryOrder(12345, null);
    assertEquals(null, result);

  }

  @Test
  public void testSmall() throws Exception {
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

    assertEquals(Short.MAX_VALUE, Util.byte2short(Util.short2byte(Short.MAX_VALUE)));
    assertEquals(65535, Util.byte2short(Util.short2byte(-1)));
    assertEquals(0, Util.byte2short(Util.short2byte(0)));
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
