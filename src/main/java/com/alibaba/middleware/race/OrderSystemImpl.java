package com.alibaba.middleware.race;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

/**
 * Created by yfy on 7/11/16.
 * OrderSystemImpl
 */
public class OrderSystemImpl implements OrderSystem {

  private Database db;

  public OrderSystemImpl() {
  }

  public void construct(Collection<String> orderFiles,
                        Collection<String> buyerFiles,
                        Collection<String> goodFiles,
                        Collection<String> storeFolders)
      throws IOException, InterruptedException {

    try {
      //System.out.println(orderFiles.size() + " " + buyerFiles.size() + " "
      //   + goodFiles.size() + " " + storeFolders.size());
      db = new Database(orderFiles, buyerFiles, goodFiles, storeFolders);
      db.construct();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public Result queryOrder(long orderId, Collection<String> keys) {
    //System.out.println("[yfy] queryOrder " + orderId + ' ' + Util.keysStr(keys));
    try {
      return db.queryOrder(orderId, keys);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  public Iterator<Result> queryOrdersByBuyer(
      long startTime, long endTime, String buyerid) {

    //System.out.printf("[yfy] queryOrderByBuyer %d %d %s\n", startTime, endTime, buyerid);
    try {
      return db.queryOrdersByBuyer(startTime, endTime, buyerid);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  public Iterator<Result> queryOrdersBySaler(
      String salerid, String goodid, Collection<String> keys) {

    //System.out.printf("[yfy] queryOrdersBySaler %s %s %s\n", salerid, goodid, Util.keysStr(keys));
    try {
      return db.queryOrdersBySaler(goodid, keys);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  public KeyValue sumOrdersByGood(String goodid, String key) {
    //System.out.println("[yfy] sumOrdersByGood " + goodid + ' ' + key);
    try {
      return db.sumOrdersByGood(goodid, key);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }
}
