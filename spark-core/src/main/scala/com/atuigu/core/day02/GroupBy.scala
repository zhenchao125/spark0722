package com.atuigu.core.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/9 9:26
  */
object GroupBy {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("GroupBy").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 7, 60, 1, 20)
       val rdd1: RDD[Int] = sc.parallelize(list1)
        val rdd2: RDD[(Int, Iterable[Int])] = rdd1.groupBy(x => x % 2)
        println(rdd2.collect().mkString(", "))
        sc.stop()
    }
}
