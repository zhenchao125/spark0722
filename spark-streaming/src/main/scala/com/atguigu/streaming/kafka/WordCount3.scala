package com.atguigu.streaming.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author lzc
  * Date 2019/12/17 16:29
  */
object WordCount3 {
    
    val map = Map[String, String](
        ConsumerConfig.GROUP_ID_CONFIG -> "streaming0722",
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092"
    )
    val topics = Set("first")
    val groupId = map(ConsumerConfig.GROUP_ID_CONFIG)
    val cluster: KafkaCluster = new KafkaCluster(map)
    
    /**
      * 借用KafkaCluster来手动读取要读取的数据的Offsets
      *
      * @return
      */
    def readOffsets(): Map[TopicAndPartition, Long] = {
        var resultMap: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long]()
        // 获取所有的分区
        val topicAndPartionEither: Either[Err, Set[TopicAndPartition]] = cluster.getPartitions(topics)
        topicAndPartionEither match {
            case Right(topicAndPartitions) =>
                // 获取每个分区的偏移量
                val topicAndPartionAndOffsets: Either[Err, Map[TopicAndPartition, Long]] = cluster.getConsumerOffsets(groupId, topicAndPartitions)
                if (topicAndPartionAndOffsets.isRight) {
                    resultMap ++= topicAndPartionAndOffsets.right.get
                } else { // 当时第一次的时候应该是所有的topic所有地方分区从offset 0开始消费
                    topicAndPartitions.foreach(tap => {
                        resultMap += tap -> 0L
                    })
                }
            case _ =>
        }
        resultMap
    }
    
    /**
      * 保存每次消费的offset, 可以在下次消费的时候从readOffsets方法可以读到
      */
    def saveOffsets(sourceDStream: InputDStream[String]) {
        sourceDStream.foreachRDD(rdd => {
            val hasOffsetRanges: HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]
            val offsetRanges: Array[OffsetRange] = hasOffsetRanges.offsetRanges
            
            var map: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long]()
            offsetRanges.foreach(offsetRange => {
                map += offsetRange.topicAndPartition() -> offsetRange.untilOffset
            })
            cluster.setConsumerOffsets(groupId, map)
        })
    }
    
    def main(args: Array[String]): Unit = {
        
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount1")
        val ssc = new StreamingContext(conf, Seconds(3))
        
        
        val sourceDStream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
            ssc,
            map,
            readOffsets(),
            (handler: MessageAndMetadata[String, String]) => handler.message()
        )
        
        sourceDStream.flatMap(_.split("\\W+")).map((_, 1)).reduceByKey(_ + _).print(100)
        
        saveOffsets(sourceDStream)  // 消费一次保存一次
        ssc.start()
        ssc.awaitTermination()
        
        
    }
    
}
