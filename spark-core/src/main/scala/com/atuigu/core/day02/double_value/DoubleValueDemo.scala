package com.atuigu.core.day02.double_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/9 10:50
  */
object DoubleValueDemo {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("DoubleValueDemo").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = 1 to 8
        val list2 = 5 to 10
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        val rdd2: RDD[Int] = sc.parallelize(list2, 2)
        // 拉链
        /*
        0: 分区数相等
        1. 每个分区的元素个数必须相等
         */
//        val rdd3: RDD[(Int, Int)] = rdd1.zip(rdd2)
        
        // 元素和元素的索引
//        val rdd3 = rdd1.zipWithIndex()
        // 1. 分区数相等 2. 每个分区内的元素可以不同
        val rdd3 = rdd1.zipPartitions(rdd2)((it1, it2) => it1.zipAll(it2, 100, 200))
//        val rdd3 = rdd1.zipPartitions(rdd2)(_.zipAll(_, 100, 200))
        rdd3.collect.foreach(println)
        // 并集
//        val rdd3: RDD[Int] = rdd1.union(rdd2)
//        val rdd3: RDD[Int] = rdd1.++(rdd2)
//        val rdd3: RDD[Int] = rdd1 ++ rdd2
        
        // 交集
//        val rdd3: RDD[Int] = rdd1.intersection(rdd2)
        // 差集
//        val rdd3: RDD[Int] = rdd1.subtract(rdd2)
//        val rdd3: RDD[(Int, Int)] = rdd1.cartesian(rdd2)
//        rdd3.collect.foreach(println)
        
        sc.stop()
        
    }
}
