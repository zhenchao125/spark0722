package com.atuigu.core.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/9 9:15
  */
object FlatMap {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("FlatMap").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        
        val rdd: RDD[Int] = sc.parallelize(list1)
//        val rdd1 = rdd.flatMap(x => List(x, x * x, x * x * x))
//        println(rdd1.collect().toList)
        val rdd1: RDD[Array[Int]] = rdd.glom()
        rdd1.collect.foreach(x => {
            println(x.mkString(","))
        })
        
        sc.stop()
    }
}
