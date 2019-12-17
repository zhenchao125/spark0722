package com.atguigu.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * Author lzc
  * Date 2019/12/17 14:38
  */
object WordCountRDD {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCountRDD")
        val ssc = new StreamingContext(conf, Seconds(3))
        val queue: mutable.Queue[RDD[Int]] = mutable.Queue[RDD[Int]]()
        val dStream: InputDStream[Int] = ssc.queueStream(queue,false)
        
        val resultDSteam: DStream[Int] = dStream.reduce(_ + _)
        resultDSteam.print()
        
        ssc.start()
        
        while(true){
            queue.enqueue(ssc.sparkContext.parallelize(1 to 100))
            Thread.sleep(2000)
        }
        ssc.awaitTermination()
    }
}
