package com.atuigu.core.day02.kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/9 14:27
  */
object ReduceByKey {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("ReduceByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(("a", 1), ("b", 2), ("a", 1), ("a", 2), ("b", 2), ("a", 3))
        val rdd1= sc.parallelize(list1, 2)
        val res: RDD[(String, Int)] = rdd1.reduceByKey((x, y) => x + y)
//        val res = rdd1.groupByKey()
//                .map(kv => (kv._1, kv._2.sum))
        res.collect.foreach(println)
        
        sc.stop()
        
    }
}
