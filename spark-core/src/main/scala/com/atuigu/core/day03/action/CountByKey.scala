package com.atuigu.core.day03.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/10 14:20
  */
object CountByKey {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("CountByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List("a", "a", "b", "b", "c", "d")
        val rdd1 = sc.parallelize(list1, 2)
        
        val rdd2 = rdd1.map((_, 1)).reduceByKey(_ + _)
//        val map: collection.Map[String, Long] = rdd1.map((_, 1)).countByKey()
//        println(map)
        
//        rdd1.collect.foreach(println)
        // 建立 connect  X
        rdd1.foreach(x => {
//            println(Thread.currentThread().getName)
            // 建立 connect  X
        
        })
        
        rdd1.foreachPartition(it => {
            // 建立连接
            it.foreach(x => {
                // 向外写
            })
        })
        sc.stop()
        
    }
}
/*
统计后的结果写入到外部存储:
1. 把结果拉倒驱动, 让后在驱动端一起往外写
    oom
2. foreach遍历的方式向外写

3.
 */