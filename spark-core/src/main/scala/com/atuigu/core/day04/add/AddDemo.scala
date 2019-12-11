package com.atuigu.core.day04.add

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/11 15:46
  */
object AddDemo {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("AddDemo").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        
        var a = 1
        
        rdd1.foreach(x => a += 1)
        println(a)
        sc.stop()
        
        
        
    }
}
/*
共享变量的更改问题:
    累加器

 */