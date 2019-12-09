package com.atuigu.core.day02.kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/9 15:21
  */
object AggByKey {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("AggByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)
        val res: RDD[(String, String)] = rdd.aggregateByKey("-")(_ + _, _ + _)
//
//        val res = rdd.aggregateByKey(Int.MinValue)(_.max(_), _ + _)
        res.collect.foreach(println)
        sc.stop()
        
    }
}
/*
reduceByKey(f)
foldByKey(zero)(f)
aggregateByKey(zero)(f1, f2)
    zero使用最多和分区数保持一致
    

    

 */