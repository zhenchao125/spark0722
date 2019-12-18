package com.atguigu.realtime.app

import com.atguigu.realtime.bean.AdsInfo
import com.atguigu.realtime.util.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

/**
  * Author atguigu
  * Date 2019/12/18 16:18
  */
object DayAreaAdsTop {
    def statDayAreaAdsTop3(adsInfoDSteam: DStream[AdsInfo]): Unit = {
        
        //1.  ((2019-12-18, 华北, ads), 1) -> ((2019-12-18, 华北, ads), 10000)
        val dayAreaAdsAndCount: DStream[((String, String, String), Int)] = adsInfoDSteam
            .map(adsInfo => ((adsInfo.dayString, adsInfo.area, adsInfo.adsId), 1))
            .updateStateByKey((seq: Seq[Int], opt: Option[Int]) => Some(seq.sum + opt.getOrElse(0)))
        
        // 2. 按照天和地区分组   ((day, area), (adsId, count))
        val resultDSteam: DStream[(String, String, List[(String, Int)])] = dayAreaAdsAndCount
            .map {
                case ((day, area, adsId), count) => ((day, area), (adsId, count))
            }
            .groupByKey
            .map {
                case ((day, area), it) =>
                    (day, area, it.toList.sortBy(-_._2).take(3))
            }
        // 3. 写入到redis中
        resultDSteam.foreachRDD(rdd => {
            rdd.foreachPartition((it: Iterator[(String, String, List[(String, Int)])]) => {
                // 1. 到redis连接
                val client: Jedis = RedisUtil.getJedisClient
                // 2. 发送数据
                /*
                数据格式:
                   (day, area, List(adsId, count))
                   
                   key                                      value
                   "day:area:adsId:"+2019-12-18             field          value
                                                            area           "{adsId: count, adsId: count}"
                  
                   
                 */
                
                it.foreach {
                    case (day, area, list) =>
                        // json4s  json for scala
                        import org.json4s.JsonDSL._
                        val jsonStr: String = JsonMethods.compact(JsonMethods.render(list))
                        client.hset(s"day:area:adsId:$day", area, jsonStr)
                }
                
                
                // 3. 关闭连接  把连接放回连接池
                client.close()
                
            })
            
        })
        
        
        resultDSteam.print(10000)
    }
}
