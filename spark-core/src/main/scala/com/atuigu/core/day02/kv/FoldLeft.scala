package com.atuigu.core.day02.kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/9 14:58
  */
object FoldLeft {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("FoldLeft").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd= sc.parallelize(Array(("c","3"), ("c","2"), ("c","4"), ("c","3"), ("c","6"), ("c","8")), 3)
        
        // foldByKey来说, 0值, 每个分区内用一次. 重点: 分区间合并的时候, 零值不参与
        val res = rdd.foldByKey("-")(_ + _)
        res.collect.foreach(println)
        
        
        sc.stop()
        
    }
}
/*
1. reduceByKey
2. foldByKey
 */