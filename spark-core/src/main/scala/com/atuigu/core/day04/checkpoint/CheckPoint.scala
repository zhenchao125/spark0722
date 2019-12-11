package com.atuigu.core.day04.checkpoint

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/11 9:18
  */
object CheckPoint {
    def main(args: Array[String]): Unit = {
//        System.setProperty("HADOOP_USER_NAME", "atguigu")
        val conf: SparkConf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
        sc.setCheckpointDir("hdfs://hadoop102:9000/ck")
        val rdd1= sc.parallelize(Array("hello" /*, "hello", "world", "hello", "atguigu", "hello", "atguigu", "atguigu"*/))
        val rdd2 = rdd1.map(x => {
            println("rdd2: " + x)
            (x, System.currentTimeMillis)
        })
        val rdd3 = rdd2.filter(x => {
            println("rdd3: " + x)
            true
        })
        /*
         碰到第一个行动算子,开始执行.
         等第一个执行结束之后, 会立即再次启动换一个新的job, 做checkpoint
         */
        rdd3.checkpoint()
        rdd3.cache()
    
    
        rdd3.collect.foreach(println)
        println("-------------------")
        rdd3.collect.foreach(println)
        println("-------------------")
        
        rdd3.collect.foreach(println)
        println("-------------------")
        rdd3.collect.foreach(println)
        
        Thread.sleep(10000000)
        
        sc.stop()
    }
}
