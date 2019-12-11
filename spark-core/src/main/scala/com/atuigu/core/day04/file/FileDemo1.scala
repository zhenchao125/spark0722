package com.atuigu.core.day04.file

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/11 11:15
  */
object FileDemo1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("FileDemo1").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd1 = sc.textFile("c:/0508")
        
        rdd1.collect
        sc.stop()
    }
}
