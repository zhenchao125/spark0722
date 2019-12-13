package com.atguigu.spark.core.project.app

import com.atguigu.spark.core.project.bean.UserVisitAction
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * Author lzc
  * Date 2019/12/13 14:25
  */
object CategoryTop10Session {
    
    /**
      * 统计每个品类的top10的活跃session
      *
      * @param sc
      * @param cids
      * @param userVisitActionRDD
      */
    def calcTop10Session(sc: SparkContext, cids: List[Long], userVisitActionRDD: RDD[UserVisitAction]): Unit = {
        // 1. 过滤: 过滤出来top10品类的点击记录
        val cidsBD: Broadcast[List[Long]] = sc.broadcast(cids)
        val filteredUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(action => action.click_category_id != -1 && cidsBD.value.contains(action.click_category_id))
        // 2. 统计每个品类的下的每个session点击记录  (cid, sessionId), 1
        val cidSessionAndOne: RDD[((Long, String), Int)] = filteredUserVisitActionRDD.map(action => ((action.click_category_id, action.session_id), 1))
        val cidAndSessionCount: RDD[(Long, (String, Int))] = cidSessionAndOne.reduceByKey(_ + _).map {
            case ((cid, sessionId), count) => (cid, (sessionId, count))
        }
        // 3. 分别排序取前10
        val top10SessionRDD: RDD[(Long, List[(String, Int)])] = cidAndSessionCount
            .groupByKey()
            .map {
                case (cid, it) =>
                    (cid, it.toList.sortBy(-_._2).take(10))
            }
        
        top10SessionRDD.collect().foreach(println)
    }
    
    
}
