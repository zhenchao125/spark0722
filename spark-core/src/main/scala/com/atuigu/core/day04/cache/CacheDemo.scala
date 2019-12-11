package com.atuigu.core.day04.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/11 8:20
  */
object CacheDemo {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val rdd1: RDD[String] = sc.parallelize(Array("hello", "hello", "world"/*, "hello", "atguigu", "hello", "atguigu", "atguigu"*/))
        val rdd2: RDD[(String, Long)] = rdd1.map(x => {
            println("rdd2: " + x)
            (x, System.currentTimeMillis)
        })
        val rdd3: RDD[(String, Long)] = rdd2.filter(x => {
            println("rdd3: " + x)
            true
        }).reduceByKey(_ + _)
        // 默认是在在内存中  . shuffule算子的结果, spark会自动缓存
        rdd3.cache()
//        rdd3.persist(StorageLevel.MEMORY_ONLY_SER)
        
        rdd3.collect.foreach(println)
        println("-------------------")
        rdd3.collect.foreach(println)
        rdd3.collect.foreach(println)
        rdd3.collect.foreach(println)
        
        Thread.sleep(10000000)
        sc.stop()
        
    }
}
