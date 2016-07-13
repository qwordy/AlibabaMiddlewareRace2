package com.alibaba.middleware.race;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Created by yfy on 7/11/16.
 * OrderSystemImpl
 */
public class OrderSystemImpl implements OrderSystem {

  private List<String> orderFilesList;

  public OrderSystemImpl() {}

  public void construct(Collection<String> orderFiles,
                        Collection<String> buyerFiles,
                        Collection<String> goodFiles,
                        Collection<String> storeFolders)
      throws IOException, InterruptedException {

    Constructor constructor = new Constructor();

    int fileId = 0;
    orderFilesList = new ArrayList<>();
    for (String file : orderFiles) {
      orderFilesList.add(file);
      constructor.readOrderFile(file, fileId);
      fileId++;
    }

  }

  public Result queryOrder(long orderId, Collection<String> keys) {
    return null;
  }

  public Iterator<Result> queryOrdersByBuyer(
      long startTime, long endTime, String buyerid) {
    return null;
  }

  public Iterator<Result> queryOrdersBySaler(
      String salerid, String goodid, Collection<String> keys) {
    return null;
  }

  public KeyValue sumOrdersByGood(String goodid, String key) {
    return null;
  }
}
