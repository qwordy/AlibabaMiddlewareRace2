package com.alibaba.middleware.race;

/**
 * Created by yfy on 7/13/16.
 * Config
 */
public class Config {

  public static int orderIndexBuffer1BlockNum = 333333;
//  public static int orderIndexBuffer1BlockNum = 333;

  // 每个桶预计放300个，容量409个, 2.73g
  public static int orderIndexSize = 666666;
//  public static int orderIndexSize = 500;

  public static int orderIndexBlockSize = 4096;

  public static int buyerIndexSize = 8000000;
//  public static int buyerIndexSize = 80000;

  // 250, 2.9g, 375, 3.6g
  public static int buyerIndexBlockSize = 366;

  public static int goodIndexSize = 4000000;
//  public static int goodIndexSize = 40000;

  // 500, 2.8g, 750, 3.7g
  public static int goodIndexBlockSize = 771;

  public static int b2bIndexSize = 152381; // 0.75

  public static int g2gIndexSize = 76191; // 0.75

  // 52.5 in 70 entrys
  public static int bg2bgIndexBlockSize = 2036;

  // 327.2 of 409, f = 0.8, 4g, 319531741 entrys
  //public static int orderIndex1Size = 976564;
//  public static int orderIndex1Size = 100;

  // 286 of 409, f = 0.7, 1.152g, 80468259 entrys
  //public static int orderIndex2Size = 281357;
//  public static int orderIndex2Size = 25;

//  //public static int b2bIndexSize = 37735;  // 0.75
//  public static int b2bIndexSize = 35461;  // 0.8
//
//  //public static int g2gIndexSize = 18867;  // 0.75
//  public static int g2gIndexSize = 17731;  // 0.8
//
//  public static int bg2bgIndexBlockSize = 8192;

//  public static int b2bIndexSize = 75471; // 0.75
//
//  public static int g2gIndexSize = 37735; // 0.75
//
//  public static int bg2bgIndexBlockSize = 4096;

  public static long orderidMax = 60767378408L;
  public static long orderidMin = 587732231;

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

//  [yfy] buyer num: 8000000
//  [yfy] good num: 4000000

//  1469862899948 [yfy] writeFile start
//  [yfy] size: 4000000 extSize: 1134804  blockSize = 666
//  1469862943698 [yfy] writeFile end
//  [yfy] size: 18867 extSize: 0


//  public static long createtimeMax = 11668409867L;

//  good 4m
//  buyer 8m
//  order 400m 2m block

  /**
   * 31:
   * 23cf629c17a
   * 30:
   * tmp/5e6e0847c8b/
   * tmp/0b63c40f834/
   * tmp/702b3e07f7b/
   * 29:
   * tmp/255f0a61e4d/
   * tmp/ad4f8b693bc/
   * tmp/9b6f625817f/
   * tmp/0b2542a378a/
   */

}
