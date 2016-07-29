package com.alibaba.middleware.race;

/**
 * Created by yfy on 7/13/16.
 * Config
 */
public class Config {

  // 每个桶预计放182个，容量409个
  public static int orderIndexSize = 732421;

  public static int orderIndexBlockSize = 4096;

  public static int buyerIndexSize = 8000000;

  public static int buyerIndexBlockSize = 512;

  public static int goodIndexSize = 4000000;

  public static int goodIndexBlockSize = 1024;

  public static long orderidMax = 60767378408L;


//  public static int eachIndexMaxCount = 60000000;
//
//  public static long orderidMin = 587732231;
//
//  public static long createtimeMax = 11668409867L;
//  public static int buyerIndexBlockBufSize = 80;
//  public static int goodIndexBlockBufSize = 160;
//
//  public static int orderIndexBlockBufSize = 1500;//640;

}
