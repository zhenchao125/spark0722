package com.atguigu.streaming.unstate

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author lzc
  * Date 2019/12/18 10:20
  */
object UnstateDemo1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UnstateDemo1")
        val ssc = new StreamingContext(conf, Seconds(3))
        
        val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 10000)
        
        val resultDSteam = dstream.transform(rdd => {
            rdd.flatMap(_.split("\\W+")).map((_, 1)).reduceByKey(_ + _)
        })
        
//        resultDSteam.print(1000)
        resultDSteam.foreachRDD(rdd => {
            /*rdd.foreachPartition(it => {
            
            })*/
            
        })
        
        ssc.start()
        ssc.awaitTermination()
        
    }
}

/*
1. steam.foreachRDD(rdd => {   // 操作RDD})

2.
 */