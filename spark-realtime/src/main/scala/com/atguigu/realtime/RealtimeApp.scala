package com.atguigu.realtime

import com.atguigu.realtime.app.DayAreaAdsTop
import com.atguigu.realtime.bean.AdsInfo
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
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
        ssc.checkpoint("realtime")
        val sourceDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getDStream(ssc, "ads_log")
        
        val adsInfoDSteam: DStream[AdsInfo] = sourceDStream.map(record => {
            // 1576655451922,华北,北京,105,2
            val split: Array[String] = record.value().split(",")
            AdsInfo(
                split(0).toLong,
                split(1),
                split(2),
                split(3),
                split(4))
        })
        
        // 需求1: 每天每地区没广告的点击率的top3
        DayAreaAdsTop.statDayAreaAdsTop3(adsInfoDSteam)
        
        ssc.start()
        ssc.awaitTermination()
        
    }
}
