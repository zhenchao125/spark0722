package com.atuigu.core.day02.kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/9 14:03
  */
object KVDemo1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("KVDemo1").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 1, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 3)
        val kvRDD: RDD[(Int, Int)] = rdd1.map((_, 1))
        println(kvRDD.partitioner)
        val resRDD: RDD[(Int, Int)] = kvRDD.partitionBy(new HashPartitioner(2))
        println(resRDD.partitioner)
        resRDD.mapPartitionsWithIndex((index, it) => it.map((index, _))).collect.foreach(println)
        
        sc.stop()
        
    }
}
