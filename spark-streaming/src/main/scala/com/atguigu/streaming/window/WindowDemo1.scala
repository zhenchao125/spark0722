package com.atguigu.streaming.window

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author lzc
  * Date 2019/12/18 11:26
  */
object WindowDemo1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UnstateDemo1")
        val ssc = new StreamingContext(conf, Seconds(3))
        ssc.checkpoint("ck4")
        ssc.socketTextStream("hadoop102", 10000)
            .flatMap(_.split("\\W+"))
            .map((_, 1))
            // 使用默认的滑动步长: 等于周期
//            .reduceByKeyAndWindow(_ + _, Seconds(9))
//            .reduceByKeyAndWindow(_ + _, Seconds(9), slideDuration = Seconds(6))
            .reduceByKeyAndWindow(_ + _, _ - _, Seconds(9), filterFunc = kv => kv._2 != 0)
            .print(100)
    
        ssc.start()
        ssc.awaitTermination()
    }
}
