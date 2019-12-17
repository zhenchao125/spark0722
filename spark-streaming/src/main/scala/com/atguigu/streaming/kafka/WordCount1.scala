package com.atguigu.streaming.kafka


import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author lzc
  * Date 2019/12/17 15:54
  */
object WordCount1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount1")
        val ssc = new StreamingContext(conf, Seconds(3))
        
        val map = Map[String, String](
            ConsumerConfig.GROUP_ID_CONFIG -> "streaming0722",
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092"
        )
        
        val kafkaDsteam: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
            ssc,
            map,
            Set("first")
        )
        kafkaDsteam
            .flatMap {
                case (_, line) => line.split("\\W+").map((_, 1))
            }
            .reduceByKey(_ + _)
            .print(1000)
        
        ssc.start()
        ssc.awaitTermination()
        
    }
}

/*
KafkaUtils
KafkaCluster
 */