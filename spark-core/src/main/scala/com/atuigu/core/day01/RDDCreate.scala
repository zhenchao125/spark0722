package com.atuigu.core.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Author lzc
  * Date 2019/12/7 16:02
  */
object RDDCreate {
    
    
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("RDDCreate")
        val sc = new SparkContext(conf)
        val arr1 = Array(30, 50, 70, 60, 10, 20)
        val sourceRDD: RDD[Int] = sc.makeRDD(arr1)
//        val sourceRDD: RDD[Int] = sc.parallelize(arr1)
    
        println(sourceRDD.collect().mkString(","))
        
        
        sc.stop()
        
    }
}

/*
如何得到RDD

1. 从scala集合中得到
2. 从外部存储系统得到
3. 从其他的RDD通过转换得到
 */