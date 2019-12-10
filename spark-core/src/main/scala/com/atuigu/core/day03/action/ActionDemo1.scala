package com.atuigu.core.day03.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/10 14:09
  */
object ActionDemo1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("ActionDemo1").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
    
//        println(rdd1.count())
//        val arr: Array[Int] = rdd1.take(4)
//        val arr: Array[Int] = rdd1.takeOrdered(3)
//        arr.foreach(println)
        
        /*rdd1.map(x => {
            println("abc")
            x
        }).filter(x => {
            println("def")
            true
        }).collect()*/
        
        sc.stop()
        
    }
}
