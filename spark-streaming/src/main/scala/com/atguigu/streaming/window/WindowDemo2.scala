package com.atguigu.streaming.window

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author lzc
  * Date 2019/12/18 11:26
  */
object WindowDemo2 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UnstateDemo1")
        val ssc = new StreamingContext(conf, Seconds(3))
        
        ssc.socketTextStream("hadoop102", 10000)
            .window(Seconds(9), Seconds(6))
            .flatMap(_.split("\\W+"))
            .map((_, 1))
            .reduceByKey(_ + _)
            //            .print(100)
            .saveAsTextFiles("tmp", "txt") // tmp-1111111.txt
        
        ssc.start()
        ssc.awaitTermination()
    }
}
