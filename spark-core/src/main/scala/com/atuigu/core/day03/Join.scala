package com.atuigu.core.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/10 10:10
  */
object Join {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Join").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        var rdd1 = sc.parallelize(Array((1, "a"), (1, "b"), (2, "c"), (4, "d")))
        var rdd2 = sc.parallelize(Array((1, "aa"),(1, "bb"), (3, "bb"), (2, "cc")), 3)
        // 内连接
//        val res: RDD[(Int, (String, String))] = rdd1.join(rdd2)
//        var res = rdd1.leftOuterJoin(rdd2)
//        val res: RDD[(Int, (Option[String], String))] = rdd1.rightOuterJoin(rdd2)
        val res = rdd1.rightOuterJoin(rdd2)
        println(res.partitions.length)
        res.collect.foreach(println)
        sc.stop()
        
    }
}
