package com.atuigu.core.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/9 10:13
  */
object Coalease {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Coalease").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        
        // 用来改变分区去
//        val rdd2 = rdd1.coalesce(1)
        // 用来增加分区
        val rdd2 = rdd1.repartition(10)
        val rdd3 = rdd2.mapPartitionsWithIndex((index, it) => it.map(x => index))
        println(rdd3.collect().toList)
        sc.stop()
        
    }
}
/*
如果减少分区, 一定还要用coalesce,  减少分区的时候可以不shuffle.
如果是增加分区, 采用repartition
 */