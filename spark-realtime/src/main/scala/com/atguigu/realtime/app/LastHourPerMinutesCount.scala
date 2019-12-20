package com.atguigu.realtime.app

import com.atguigu.realtime.bean.AdsInfo
import com.atguigu.realtime.util.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds}
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

/**
  * Author atguigu
  * Date 2019/12/20 9:15
  */
object LastHourPerMinutesCount {
    def statLastHourCount(adsInfoDSteam: DStream[AdsInfo]): Unit = {
        // 1. 计算出来每个广告每分钟的点击量
        val adsIdAndHMCount: DStream[(String, Iterable[(String, Int)])] = adsInfoDSteam
            .window(Minutes(60), Seconds(5))
            .map(adsInfo => {
                ((adsInfo.adsId, adsInfo.hmString), 1)
            })
            .reduceByKey(_ + _)
            .map {
                case ((adsId, hm), count) => (adsId, (hm, count))
            }
            .groupByKey()
            .mapValues(it => it.toList.sortBy(_._1))
        
        // 2. 写入到redis
        adsIdAndHMCount.foreachRDD(rdd => {
            rdd.foreachPartition(it => {
                val list: List[(String, Iterable[(String, Int)])] = it.toList
                if (list.size > 0) {
                    val client: Jedis = RedisUtil.getJedisClient
                    // json4s  里面的一些隐式转换
                    import org.json4s.JsonDSL._
                    // 迭代器内所有的数据, 一次性的写入到redis
                    val map: Map[String, String] = list.toMap.map {
                        case (ads, it1) => (ads, JsonMethods.compact(JsonMethods.render(it1)))
                    }
                    // 用来在scala的集合和java的集合中的隐式转换用
                    import scala.collection.JavaConversions._
                    println(map)
                    client.hmset("last:hour:ads:click", map)
                    client.close()
                }
            })
            
        })
        
        adsIdAndHMCount.print(1000)
    }
}
