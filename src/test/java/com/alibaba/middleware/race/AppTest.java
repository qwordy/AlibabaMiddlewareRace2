package com.alibaba.middleware.race;

import org.junit.Test;

import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;

/**
 * Created by yfy on 7/11/16.
 * Test
 */
public class AppTest {

  @Test
  public void queryBig() throws Exception {
    OrderSystem os = constructBig();

    // queryOrder
    OrderSystem.Result result = os.queryOrder(606092157, Arrays.asList("buyername"));
    assertEquals("晋恿吾", result.get("buyername").valueAsString());

    result = os.queryOrder(604911336, Arrays.asList("buyerid"));
    assertEquals("tp-9d00-3b1cf5d41ff5", result.get("buyerid").valueAsString());

    // queryOrdersByBuyer
    os.queryOrdersByBuyer(1471183448, 1483854721, "ap-9cfb-1009514ce5f1");

    // queryOrdersBySaler
    Iterator<OrderSystem.Result> iter = os.queryOrdersBySaler(
        "ay-9f78-e1fe3f5fb5ce",
        "al-814a-e3bba7062bdd",
        Arrays.asList("good_name", "a_o_12490", "a_o_4082", "buyerid", "a_o_9238"));

    OrderSystem.KeyValue kv = os.sumOrdersByGood("al-950f-5924be431212", "a_g_10839");
    assertEquals(null, kv);

    // sumOrdersByGood
    kv = os.sumOrdersByGood("dd-8ad6-8de99e8d7dad", "amount");
    assertEquals(735, kv.valueAsLong());

    kv = os.sumOrdersByGood("dd-b9e1-77c52c63fffa", "price");
    assertEquals(455880.3135284825, kv.valueAsDouble(), 1e-6);

  }

  private OrderSystem constructBig() throws Exception {
    OrderSystem os = new OrderSystemImpl();
    os.construct(
        Arrays.asList(fn("order.0.0"), fn("order.0.3"), fn("order.1.1"), fn("order.2.2")),
        Arrays.asList(fn("buyer.0.0"), fn("buyer.1.1")),
        Arrays.asList(fn("good.0.0"), fn("good.1.1"), fn("good.2.2")),
        Arrays.asList("/home/yfy/middleware/prerun_data"));
    return os;
  }

  private String fn(String file) {
    return "/home/yfy/middleware/prerun_data/" + file;
  }

  @Test
  public void constructMedium() throws Exception {
    OrderSystem os = new OrderSystemImpl();
    os.construct(Arrays.asList("order_records.txt"),
        Arrays.asList("buyer_records.txt"),
        Arrays.asList("good_records.txt"), null);
  }

  @Test
  public void testMedium() throws Exception {
    OrderSystem os = new OrderSystemImpl();
    os.construct(Arrays.asList("order_records.txt"),
        Arrays.asList("buyer_records.txt"),
        Arrays.asList("good_records.txt"),
        Arrays.asList("/home/yfy/middleware/race2"));

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

    Iterator<OrderSystem.Result> iter = os.queryOrdersByBuyer(1408867965, 1508867965,
        "ap_855a4497-5614-401f-97be-45a6c6e8936c");
    int count = 0;
    while (iter.hasNext()) {
      iter.next();
      count++;
    }
    assertEquals(33, count);

    iter = os.queryOrdersByBuyer(1466441697, 1470031858,
        "tb_7a1f9032-e715-4c84-abaa-2405e7579408");
    count = 0;
    while (iter.hasNext()) {
      result = iter.next();
      //System.out.println(result.orderId() + " " + result.get("createtime").valueAsLong());
      count++;
    }
    assertEquals(21, count);

    iter = os.queryOrdersBySaler("", "good_e3111b68-638b-4a5b-89ef-15f522171b9f", null);
    count = 0;
    while (iter.hasNext()) {
      result = iter.next();
      //System.out.println(result.orderId());
      count++;
      if (count == 1) {
        assertEquals(2982453, result.orderId());
        assertEquals(4.21, result.get("offprice").valueAsDouble(), 1e-6);
      }
      if (count == 22) {
        assertEquals(3009294, result.orderId());
        assertEquals("云集", result.get("buyername").valueAsString());
      }
    }
    assertEquals(22, count);

    OrderSystem.KeyValue kv = os.sumOrdersByGood(
        "aliyun_6371c5b3-29e0-48f1-9e1f-602b034122a6", "amount");
    assertEquals(8494, kv.valueAsLong());
    assertEquals(8494, kv.valueAsDouble(), 1e-6);

    kv = os.sumOrdersByGood(
        "aliyun_6371c5b3-29e0-48f1-9e1f-602b034122a6", "price");
    assertEquals(398.08, kv.valueAsDouble(), 1e-6);
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
