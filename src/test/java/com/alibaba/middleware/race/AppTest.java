package com.alibaba.middleware.race;

import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.Exchanger;

import static org.junit.Assert.assertEquals;

/**
 * Created by yfy on 7/11/16.
 * Test
 */
public class AppTest {

  @Test
  public void queryBig() throws Exception {
    final OrderSystem os = constructBig();

    Runnable r = new Runnable() {
      @Override
      public void run() {
        try {
          for (int times = 0; times < 3; times++) {
            // queryOrder
    OrderSystem.Result result = os.queryOrder(606092157, null);
    assertEquals(606092157, result.orderId());
    assertEquals("wx-ae52-539368e70aaa", result.get("buyerid").valueAsString());
    assertEquals(false, result.get("done").valueAsBoolean());
    assertEquals("晋恿吾", result.get("buyername").valueAsString());

    result = os.queryOrder(604911336, Arrays.asList("buyerid"));
    assertEquals("tp-9d00-3b1cf5d41ff5", result.get("buyerid").valueAsString());

    result = os.queryOrder(590107063, Arrays.asList("a_g_12146"));
    assertEquals(590107063, result.orderId());
    assertEquals(null, result.get("a_g_12146"));

    result = os.queryOrder(608178205, Arrays.asList("a_g_17779"));
    assertEquals(false, result.get("a_g_17779").valueAsBoolean());

    result = os.queryOrder(589733122, new ArrayList<String>());
    assertEquals(589733122, result.orderId());
    assertEquals(null, result.get("buyerid"));

    result = os.queryOrder(606473320, Arrays.asList("price", "a_g_10209"));
    assertEquals(7495.452620928783, result.get("price").valueAsDouble(), 1e-6);
    assertEquals(7, result.get("a_g_10209").valueAsLong());

    result = os.queryOrder(593188936, null);
    assertEquals(11, result.get("amount").valueAsLong());

    result = os.queryOrder(607050270, null);
    assertEquals(607050270, result.orderId());

    // queryOrder
    result = os.queryOrder(606092157, null);
    assertEquals(606092157, result.orderId());
    assertEquals("wx-ae52-539368e70aaa", result.get("buyerid").valueAsString());
    assertEquals(false, result.get("done").valueAsBoolean());
    assertEquals("晋恿吾", result.get("buyername").valueAsString());

    result = os.queryOrder(604911336, Arrays.asList("buyerid"));
    assertEquals("tp-9d00-3b1cf5d41ff5", result.get("buyerid").valueAsString());

    result = os.queryOrder(590107063, Arrays.asList("a_g_12146"));
    assertEquals(590107063, result.orderId());
    assertEquals(null, result.get("a_g_12146"));

    result = os.queryOrder(608178205, Arrays.asList("a_g_17779"));
    assertEquals(false, result.get("a_g_17779").valueAsBoolean());

    result = os.queryOrder(589733122, new ArrayList<String>());
    assertEquals(589733122, result.orderId());
    assertEquals(null, result.get("buyerid"));

    result = os.queryOrder(606473320, Arrays.asList("price", "a_g_10209"));
    assertEquals(7495.452620928783, result.get("price").valueAsDouble(), 1e-6);
    assertEquals(7, result.get("a_g_10209").valueAsLong());

    result = os.queryOrder(593188936, null);
    assertEquals(11, result.get("amount").valueAsLong());

    result = os.queryOrder(607050270, null);
    assertEquals(607050270, result.orderId());

    // queryOrdersByBuyer
    Iterator<OrderSystem.Result> iter =
        os.queryOrdersByBuyer(1462018520, 1473999229, "wx-a0e0-6bda77db73ca");
    result = iter.next(); // 1
    assertEquals(607895670, result.orderId());
    assertEquals("波兰最高监察院职业。", result.get("good_name").valueAsString());
    assertEquals(0.803, result.get("a_b_6857").valueAsDouble(), 1e-6);
    result = iter.next();  // 2
    assertEquals(607807292, result.orderId());
    assertEquals("强仁乌布本田公司。韦尔奇西澳县底镇：攀登",
        result.get("address").valueAsString());
    assertEquals(9590.908570420032, result.get("price").valueAsDouble(), 1e-6);
    for (int i = 0; i < 17; i++)
      result = iter.next();
    assertEquals(587818574, result.orderId());

    for (int i = 0; i < 300; i++) {
      //System.out.println(i);
      iter = os.queryOrdersByBuyer(1462018520, 1473999229, "wx-a0e0-6bda77db73ca");
      result = iter.next(); // 1
      assertEquals(607895670, result.orderId());
      assertEquals("波兰最高监察院职业。", result.get("good_name").valueAsString());
      assertEquals(0.803, result.get("a_b_6857").valueAsDouble(), 1e-6);
      result = iter.next();  // 2
      assertEquals(607807292, result.orderId());
      assertEquals("强仁乌布本田公司。韦尔奇西澳县底镇：攀登",
          result.get("address").valueAsString());
      assertEquals(9590.908570420032, result.get("price").valueAsDouble(), 1e-6);
      for (int j = 0; j < 17; j++)
        result = iter.next();
      assertEquals(587818574, result.orderId());

      iter = os.queryOrdersByBuyer(1470285742, 1478898941, "tp-a20e-8248d4665332");
      result = iter.next();
      assertEquals(627590607, result.orderId());
      assertEquals(627590607, result.get("orderid").valueAsLong());
      assertEquals("仉律", result.get("buyername").valueAsString());
      assertEquals("蜡嘴全反射。", result.get("good_name").valueAsString());
    }

    // queryOrdersBySaler
    iter = os.queryOrdersBySaler(
        "almm-8f6a-3e6a9697a0f9",
        "aye-8d0d-57e792eb1371",
        Arrays.asList("a_b_19123"));
    result = iter.next();
    assertEquals("c51c1ce6-8d10-401a-ae5e-4f4023911cf3",
        result.get("a_b_19123").valueAsString());
    assertEquals(587983792, result.orderId());

    iter = os.queryOrdersBySaler(
        "ay-bb53-b150818332f2",
        "al-b702-2c34aeaa78cb",
        Arrays.asList("a_b_19123", "a_o_12490", "a_b_26525", "description"));
    result = iter.next();
    assertEquals(587999455, result.orderId());
    result = iter.next();
    assertEquals(588610606, result.orderId());

    iter = os.queryOrdersBySaler(null, "gd-b972-6926df8128c3",
        Arrays.asList("a_o_30709", "a_g_32587"));
    result = iter.next(); // 1
    assertEquals(588368978, result.orderId());
    assertEquals(6, result.get("a_g_32587").valueAsLong());
    assertEquals(null, result.get("a_o_30709"));
    iter.next(); // 2
    result = iter.next(); // 3
    assertEquals("e6efe5a9-5936-4d81-8d7b-ae14005584bf",
        result.get("a_o_30709").valueAsString());
    int count = 0;
    while (iter.hasNext()) {
      iter.next();
      count++;
    }
    assertEquals(53, count);

    iter = os.queryOrdersBySaler(null, "dd-b175-b63fc4e111cc", null);

    // sumOrdersByGood
    OrderSystem.KeyValue kv = os.sumOrdersByGood("al-950f-5924be431212", "a_g_10839");
    assertEquals(null, kv);

    kv = os.sumOrdersByGood("dd-8ad6-8de99e8d7dad", "amount");
    assertEquals(735, kv.valueAsLong());

    kv = os.sumOrdersByGood("dd-b9e1-77c52c63fffa", "price");
    assertEquals(455880.3135284825, kv.valueAsDouble(), 1e-6);

    kv = os.sumOrdersByGood("al-8162-0492cff4394c", "amount");
    assertEquals(552, kv.valueAsLong());

    kv = os.sumOrdersByGood("dd-a665-acd638b44e92", "price");
    assertEquals(117811.2897340, kv.valueAsDouble(), 1e-6);

    kv = os.sumOrdersByGood("al-9c4c-ac9ed4b6ad35", "offprice");
    assertEquals(235886.19656021666, kv.valueAsDouble(), 1e-6);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    };

    List<Thread> list = new ArrayList<>();
    for (int i = 0; i < 15; i++) {
      Thread thread = new Thread(r);
      thread.start();
      list.add(thread);
    }
    for (Thread thread : list)
      thread.join();

  }

  private OrderSystem constructBig() throws Exception {
    OrderSystem os = new OrderSystemImpl();
    os.construct(
        Arrays.asList(d1("order.0.0"), d2("order.0.3"), d1("order.1.1"), d3("order.2.2")),
        Arrays.asList(d1("buyer.0.0"), d2("buyer.1.1")),
        Arrays.asList(d1("good.0.0"), d2("good.1.1"), d3("good.2.2")),
        Arrays.asList("diskk1", "diskk2", "diskk3"));
    return os;
  }

  @Test()
  public void testConstructBig() throws Exception {
    constructBig();
  }

  private String d1(String file) {
    return "diskk1/" + file;
  }

  private String d2(String file) {
    return "diskk2/" + file;
  }

  private String d3(String file) {
    return "diskk3/" + file;
  }

  private String fn(String file) {
    return "../pr1run_data/" + file;
  }

  @Test
  public void constructSim() throws Exception {
    OrderSystem os = new OrderSystemImpl();
    os.construct(Arrays.asList("data/1order0"),
        Arrays.asList("data/buyer"),
        Arrays.asList("data/good"),
        Arrays.asList("data", "data", "data"));
  }

  @Test
  public void constructMedium() throws Exception {
    OrderSystem os = new OrderSystemImpl();
    os.construct(Arrays.asList("order_records.txt"),
        Arrays.asList("buyer_records.txt"),
        Arrays.asList("good_records.txt"),
        Arrays.asList(
            "/home/yfy/middleware/race2",
            "/home/yfy/middleware/race2",
            "/home/yfy/middleware/race2"));
  }

  @Test
  public void testMedium() throws Exception {
    OrderSystem os = new OrderSystemImpl();
    os.construct(Arrays.asList("order_records.txt"),
        Arrays.asList("buyer_records.txt"),
        Arrays.asList("good_records.txt"),
        Arrays.asList(
            "/home/yfy/middleware/race2",
            "/home/yfy/middleware/race2",
            "/home/yfy/middleware/race2"));

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
//    iter = os.queryOrdersByBuyer(1406441697, 1490031858,
//        "tb_7a1f9032-e715-4c84-abaa-2405e7579408");
    count = 0;
    while (iter.hasNext()) {
      result = iter.next();
      //System.out.println(result.orderId() + " " + result.get("createtime").valueAsLong());
      count++;
    }
    //assertEquals(21, count);

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
    //assertEquals(Long.MAX_VALUE, Util.byte2long(Util.long2byte(Long.MAX_VALUE)));
    //assertEquals(Long.MIN_VALUE, Util.byte2long(Util.long2byte(Long.MIN_VALUE)));
    //assertEquals(-1, Util.byte2long(Util.long2byte(-1)));
    //assertEquals(0, Util.byte2long(Util.long2byte(0)));

    //assertEquals(Integer.MAX_VALUE, Util.byte2int(Util.int2byte(Integer.MAX_VALUE)));
    //assertEquals(Integer.MIN_VALUE, Util.byte2int(Util.int2byte(Integer.MIN_VALUE)));
    //assertEquals(-1, Util.byte2int(Util.int2byte(-1)));
    //assertEquals(0, Util.byte2int(Util.int2byte(0)));

    //assertEquals(Short.MAX_VALUE, Util.byte2short(Util.short2byte(Short.MAX_VALUE)));
    //assertEquals(65535, Util.byte2short(Util.short2byte(-1)));
    //assertEquals(0, Util.byte2short(Util.short2byte(0)));

    byte[] b = new byte[5];
    Util.longToByte4(4000000000L, b, 1);
    assertEquals(4000000000L, Util.byte4ToLong(b, 1));

    assertEquals(250, (int) (((byte) 250) & 0xff));
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
    System.out.println((long) 0xfffffff << 6);
    System.out.println((int) (0xfffffffffL >> 4));
    byte[][] bufs = new byte[10][];
    System.out.println(bufs.length);
    System.out.println(bufs[0].length);
  }

  @Test
  public void fs() {
    try {
      RandomAccessFile f = new RandomAccessFile("test", "rw");
      f.setLength(400000000);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void read() throws Exception {

    BufferedInputStream bis =
        new BufferedInputStream(new FileInputStream("test"));
    RandomAccessFile fd = new RandomAccessFile("test", "r");
    int b;
//      while ((b = bis.read()) != -1);
    byte[] buf = new byte[8192];
    long count = 0;
    while (bis.read(buf) != -1) ;// && count < 4000000000L)
//      count += 8192;
//    while (fd.read(buf) != -1);

  }

  @Test
  public void disk() {
    try {
      RandomAccessFile f = new RandomAccessFile("test", "rw");
      //f.setLength(8000000000L);
      byte[] buf = new byte[8000];
      for (int i = 0; i < buf.length; i++)
        buf[i] = (byte) (Math.random() * 1000);

      Arrays.fill(buf, (byte) 5);
      buf[3] = 99;

      long t0, t1, ts = 0;

      int sum = 0, step = 4096;
      for (long i = 0; i < 4000000000L; i += step) {
        t0 = System.currentTimeMillis();
//        for (int j = 0; j < step; j++)
//          buf[j] = (byte) (Math.random()*1000);
        f.seek(i + 1400);
        f.write(buf, 0, 2000);
        sum += buf[5];
        t1 = System.currentTimeMillis();
        ts += t1 - t0;
        if (i % 8000000 == 0) {
          System.out.println(ts + " ");
          ts = 0;
        }
      }
      System.out.println(sum);

      f.seek(50000);
      System.out.println(f.read());

//      System.out.println(System.currentTimeMillis());
//      f.seek(500);
//      f.write(buf);
//      f.seek(500000);
//      f.read();
//      System.out.println(System.currentTimeMillis());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void buildData() throws Exception {
    PrintWriter pw = new PrintWriter("data/order0");
    for (int i = 0; i < 20000000; i++) {
      pw.print("orderid:");
      pw.print(String.valueOf((long) (Math.random() * 9999999999L)));
      pw.print('\t');
      pw.print("createtime:");
      pw.print(String.valueOf((long) (Math.random() * 9999999999L)));
      pw.print("\tbuyerid:");
      pw.print(randStr());
      pw.print("\tgoodid:");
      pw.print(randStr());
      pw.print("\tamount:12\tdone:true\ta_o_4699:-34842\ta_o_3337:227e8faf-defa-42ce-9725-28dca1bdb785\ta_o_22304:0.024\ta_o_12490:-1640\n");
    }
  }

  private String randStr() {
    int len = (int) (Math.random() * 12) + 8;
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < len; i++)
      sb.append((char) (Math.random() * (122 - 65) + 65));
    return sb.toString();
  }

  @Test
  public void newByte() {
    //byte[] b = new byte[400000000];
//    byte[][] bb = new byte[200000][];
//    for (int i = 0; i < 200000; i++)
//      bb[i] = new byte[4000];
    byte[] b = new byte[16000];
    System.out.println(System.currentTimeMillis());
    for (int i = 0; i < 16000; i++)
      b[i] = (byte) i;
    System.out.println(System.currentTimeMillis());
  }

  @Test
  public void col() {
    for (String s : getCol())
      s.length();
  }

  private Collection<String> getCol() {
    System.out.println(1);
    return Arrays.asList("aa", "bb");
  }

//  @Test
//  public void clhm() {
//    EvictionListener<Integer, Integer> listener = new EvictionListener<Integer, Integer>() {
//      @Override
//      public void onEviction(Integer key, Integer value) {
//        System.out.println(key + " " + value);
//      }
//    };
//
//    ConcurrentLinkedHashMap<Integer, Integer> cache =
//        new ConcurrentLinkedHashMap.Builder<Integer, Integer>()
//            .maximumWeightedCapacity(5)
//            .listener(listener)
//            .build();
//    for (int i = 0; i < 20; i++)
//      cache.put(i, i);
//    cache.get(17);
//    cache.put(3, 3);
//
//    for (int key : cache.ascendingKeySet())
//      System.out.println(key);
//  }

  @Test
  public void fill() {
//    short[] b = new short[2000000];
//    Arrays.fill(b, (short) 8);
    System.out.println("helloworld");
  }

  @Test
  public void cutCase() throws Exception {
    BufferedReader br = new BufferedReader(
        new FileReader("../prerun_data/case.0"));
    BufferedWriter bw = new BufferedWriter(
        new FileWriter("../prerun_data/case"));
    for (int i = 0; i < 100000000; i++)
      bw.write(br.read());
  }

  @Test
  public void aloc() throws Exception {
    Thread.sleep(10000);
//    byte[][] b = new byte[1000000][];
//    for (int i = 0; i < 1000000; i++)
//      b[i] = new byte[100];
    List<byte[]> list = new ArrayList<>();
    for (int i = 0; i < 1000000; i++)
      list.add(new byte[100]);
    Thread.sleep(10000);
  }

  @Test
  public void directMem() {
    try {
      FileInputStream fis = new FileInputStream("order_records.txt");
      FileChannel fc = fis.getChannel();
      System.out.println(fc.size());
      ByteBuffer buffer = ByteBuffer.allocateDirect(20);
      ByteBuffer buffer2 = ByteBuffer.allocateDirect(20);
      fc.read(buffer);
      byte[] b = new byte[10];

      buffer.position(10);
      buffer.get(b);
      printBytes(b);

      buffer.position(0);
      buffer.get(b);
      printBytes(b);

      buffer.position(5);
      buffer.get(b);
      printBytes(b);

      fc.read(buffer2);
      buffer2.position(0);
      buffer2.get(b);
      printBytes(b);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void printBytes(byte[] b) {
    for (int i = 0; i < b.length; i++)
      System.out.print((char) b[i]);
    System.out.println();
  }

}
