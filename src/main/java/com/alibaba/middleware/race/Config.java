package com.alibaba.middleware.race;

/**
 * Created by yfy on 7/13/16.
 * Config
 */
public class Config {

  // 每个桶预计放182个，容量409个
  //public static int orderIndexSize = 732421;
  public static int orderIndexSize = 100;

  public static int orderIndexBlockSize = 4096;

  public static int buyerIndexSize = 8000000;

  public static int buyerIndexBlockSize = 512;

  public static int goodIndexSize = 4000000;

  public static int goodIndexBlockSize = 1024;

  public static long orderidMax = 60767378408L;

  //[yfy] buyer max orderNum 246
  //[yfy] good max orderNum 468

//  [yfy] order num: 400000000
//  [yfy] orderid max: 60767378408 min: 587732231
//  [yfy] buyer max order num: 246
//  [yfy] dist: 6355815 637708 462424 499476 44577 0 0 0 0 0 0 0
//  [yfy] good max order num: 468
//  [yfy] dist: 419677 2778409 106809 194374 120701 106557 129251 133620 10586 16 0 0
//  [yfy] buyer max len 20 min len 20
//  [yfy] good max len 21 min len 20

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
