package com.atguigu.streaming.withstate

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author lzc
  * Date 2019/12/18 10:29
  */
object WithStateDemo {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UnstateDemo1")
        val ssc = new StreamingContext(conf, Seconds(3))
        ssc.checkpoint("ck2")
        ssc.socketTextStream("hadoop102", 10000)
            .flatMap(_.split("\\W+"))
            .map((_, 1))
            .updateStateByKey((seq: Seq[Int], option: Option[Long]) =>
                Some(seq.sum + option.getOrElse(0L)))
            
            .print(100)
        
        ssc.start()
        ssc.awaitTermination()
    }
}
