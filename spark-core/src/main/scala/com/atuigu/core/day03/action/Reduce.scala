package com.atuigu.core.day03.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/10 14:36
  */
object Reduce {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Reduce").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
//        val list1 = List(30, 50, 70, 60, 10, 20)
        val list1 = List("a", "b", "c", "d", "e", "f")
        val rdd1= sc.parallelize(list1, 3)
        
//        val sum: Int = rdd1.reduce(_ + _)
//        val sum: Int = rdd1.fold(0)(_ + _)
//        val sum: Int = rdd1.aggregate(1)(_ + _, _ + _)
        val sum: String = rdd1.aggregate("-")(_ + _, _ + _)
        println(sum)
        
        sc.stop()
        
    }
}
