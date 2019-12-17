package com.atguigu.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author lzc
  * Date 2019/12/17 13:58
  */
object WordCount1 {
    def main(args: Array[String]): Unit = {
        // 1. 创建一个SteamingContext
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount1")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(1))
        // 2. 创建一个DSteam 流
        val socketSteam: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
        // 3. 对流进行处理
        val resultDSteam: DStream[(String, Int)] = socketSteam
            .flatMap(line => line.split("\\W+"))
            .map((_, 1))
            .reduceByKey(_ + _)
        
        resultDSteam.print(100)
        
        // 4.启动流
        ssc.start()
        
        // 5. 阻止当前线程退出
        ssc.awaitTermination()
        
    }
}
