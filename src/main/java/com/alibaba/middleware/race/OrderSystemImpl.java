package com.alibaba.middleware.race;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Created by yfy on 7/11/16.
 * OrderSystemImpl
 */
public class OrderSystemImpl implements OrderSystem {

  private Database db;

  private List<String> orderFilesList;

  public OrderSystemImpl() {}

  public void construct(Collection<String> orderFiles,
                        Collection<String> buyerFiles,
                        Collection<String> goodFiles,
                        Collection<String> storeFolders)
      throws IOException, InterruptedException {

    try {
      db = new Database(orderFiles, buyerFiles, goodFiles, storeFolders);
      db.construct();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public Result queryOrder(long orderId, Collection<String> keys) {
    try {
      return db.queryOrder(orderId, keys);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  public Iterator<Result> queryOrdersByBuyer(
      long startTime, long endTime, String buyerid) {
    try {
      return db.queryOrdersByBuyer(startTime, endTime, buyerid);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  public Iterator<Result> queryOrdersBySaler(
      String salerid, String goodid, Collection<String> keys) {
    return null;
  }

  public KeyValue sumOrdersByGood(String goodid, String key) {
    return null;
  }
}
