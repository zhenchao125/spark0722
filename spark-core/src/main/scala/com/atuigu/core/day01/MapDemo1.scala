package com.atuigu.core.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/7 16:40
  */
object MapDemo1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("MapDemo1").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val arr1 = Array(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(arr1)
        
        val rdd2: RDD[Int] = rdd1.map(x => x * x)
        println(rdd2.collect().mkString(", "))
        
        sc.stop()
        
    }
}
