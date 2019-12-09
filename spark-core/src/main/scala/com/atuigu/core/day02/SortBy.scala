package com.atuigu.core.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/9 10:31
  */
object SortBy {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("SortBy").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 5, 7, 60, 10, 2)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        
        val rdd2: RDD[Int] = rdd1.sortBy(x => x % 2, true)
        rdd2.collect().foreach(println)
        
        sc.stop()
        
    }
}
