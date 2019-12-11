package com.atuigu.core.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/10 16:56
  */
object OneDemo {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("OneDemo").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        rdd1.map(x => {
            println(x)
            x
        }).sortBy(x => x)
        
        Thread.sleep(100000000)
        sc.stop()
        
    }
}
