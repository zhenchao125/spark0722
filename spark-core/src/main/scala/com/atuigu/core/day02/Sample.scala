package com.atuigu.core.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/9 9:54
  */
object Sample {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Sample").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        
        val list1 = List(30, 50, 70, 60, 10, 20, 700, 600, 100, 200, 50, 70, 60, 10, 20, 700, 600, 100, 200)
        val rdd1: RDD[Int] = sc.parallelize(list1)
        val rdd2: RDD[Int] = rdd1.distinct(3)
        
//        val rdd2: RDD[Int] = rdd1.sample(true, 1)
        println(rdd2.collect().mkString(", "))
        sc.stop()
        
    }
}
