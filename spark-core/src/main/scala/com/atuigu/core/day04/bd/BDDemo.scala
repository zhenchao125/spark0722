package com.atuigu.core.day04.bd

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/11 16:46
  */
object BDDemo {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("BDDemo").setMaster("local[4]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 4)
        val arr= Array(30, 50)
        val bdArr: Broadcast[Array[Int]] = sc.broadcast(arr)
        
        val rdd2: RDD[Int] = rdd1.filter(x => bdArr.value.contains(x))
        rdd2.collect.foreach(println)
        
        Thread.sleep(1000000)
        sc.stop()
        
        
        
    }
}
/*
解决变量的共享问题:

累加器
    写的的问题

广播变量
    大变量的读问题
    一个executor一份
 */