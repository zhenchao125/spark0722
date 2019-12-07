package com.atuigu.core.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/7 14:03
  */
object WordCount {
    def main(args: Array[String]): Unit = {
        // 1. 创建一个SparkContext
        val conf: SparkConf = new SparkConf()
            .setAppName("WordCount")
            .setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)
        // 2. 从sc得到一个RDD
        val sourceRDD: RDD[String] = sc.textFile("hdfs://hadoop102:9000/input")
        
        // 3. 对RDD做各种转换
        var rdd = sourceRDD.flatMap(_.split("\\W+"))
            .map((_, 1))
            .reduceByKey(_ + _)
        
        // 4. 调用一个行动算子   把execuor中的值收集到driver中
        val res: Array[(String, Int)] = rdd.collect()
        println(res.mkString(", "))
        // 5. 关闭sc
        sc.stop()
    }
}
