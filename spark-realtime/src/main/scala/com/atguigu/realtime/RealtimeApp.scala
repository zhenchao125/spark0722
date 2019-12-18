package com.atguigu.realtime

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.{SparkConf, streaming}
import realtime.util.MyKafkaUtil

/**
  * Author atguigu
  * Date 2019/12/18 15:27
  */
object RealtimeApp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RealtimeApp")
        val ssc = new StreamingContext(conf, streaming.Seconds(5))
        val sourceDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getDStream(ssc, "ads_log")
        
        sourceDStream.map(_.value()).print()
        
        
        ssc.start()
        ssc.awaitTermination()
        
    }
}
