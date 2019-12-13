package com.atguigu.spark.core.project.app

import com.atguigu.spark.core.project.bean.{CategorySession, UserVisitAction}
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

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
    
    
    /**
      * 统计每个品类的top10的活跃session
      *
      * @param sc
      * @param cids
      * @param userVisitActionRDD
      */
    def calcTop10Session_1(sc: SparkContext, cids: List[Long], userVisitActionRDD: RDD[UserVisitAction]): Unit = {
        // 1. 过滤: 过滤出来top10品类的点击记录
        val cidsBD: Broadcast[List[Long]] = sc.broadcast(cids)
        val filteredUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(action => action.click_category_id != -1 && cidsBD.value.contains(action.click_category_id))
        // 2. 统计每个品类的下的每个session点击记录  (cid, sessionId), 1
        val cidSessionAndOne: RDD[((Long, String), Int)] = filteredUserVisitActionRDD.map(action => ((action.click_category_id, action.session_id), 1))
        val cidAndSessionCount: RDD[(Long, (String, Int))] = cidSessionAndOne.reduceByKey(_ + _).map {
            case ((cid, sessionId), count) => (cid, (sessionId, count))
        }
        // 为了避免重复计算, 对重复时候用的RDD做缓存
        cidAndSessionCount.persist(StorageLevel.MEMORY_ONLY_SER)
        cids.foreach(cid => {
            val result = cidAndSessionCount
                .filter {
                    case (ccid, (_, _)) => ccid == cid
                }
                .sortBy(-_._2._2)
                .take(10)
            // 打印, 写入到外部存储
            result.foreach(println)
            println("-----------------")
            
        })
    }
    
    /**
      * 统计每个品类的top10的活跃session
      *
      * @param sc
      * @param cids
      * @param userVisitActionRDD
      */
    def calcTop10Session_2(sc: SparkContext, cids: List[Long], userVisitActionRDD: RDD[UserVisitAction]): Unit = {
        // 1. 过滤: 过滤出来top10品类的点击记录
        val cidsBD: Broadcast[List[Long]] = sc.broadcast(cids)
        val filteredUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(action => action.click_category_id != -1 && cidsBD.value.contains(action.click_category_id))
        // 2. 统计每个品类的下的每个session点击记录  (cid, sessionId), 1
        val cidSessionAndOne: RDD[((Long, String), Int)] = filteredUserVisitActionRDD.map(action => ((action.click_category_id, action.session_id), 1))
        val cidAndSessionCount: RDD[(Long, (String, Int))] = cidSessionAndOne.reduceByKey(_ + _).map {
            case ((cid, sessionId), count) => (cid, (sessionId, count))
        }
        // 3.
        val top10SessionRDD: RDD[mutable.TreeSet[CategorySession]] = cidAndSessionCount
            .groupByKey()
            .map {
                case (cid, it) =>
                    // 只存储10个元素, 当长度超过10的时候, 截取前10个
                    var set: mutable.TreeSet[CategorySession] = mutable.TreeSet[CategorySession]()
                    it.foreach {
                        case (sid, count) =>
                            set += CategorySession(cid, sid, count)
                            if (set.size > 10) set = set.take(10)
                        
                    }
                    set
            }
        
        top10SessionRDD.collect.foreach(println)
    }
    
    /**
      * 统计每个品类的top10的活跃session
      * 为了减少shuffle的次数
      * @param sc
      * @param cids
      * @param userVisitActionRDD
      */
    def calcTop10Session_3(sc: SparkContext, cids: List[Long], userVisitActionRDD: RDD[UserVisitAction]): Unit = {
        // 1. 过滤: 过滤出来top10品类的点击记录
        val cidsBD: Broadcast[List[Long]] = sc.broadcast(cids)
        val filteredUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(action => action.click_category_id != -1 && cidsBD.value.contains(action.click_category_id))
        // 2. 统计每个品类的下的每个session点击记录  (cid, sessionId), 1
        val cidSessionAndOne: RDD[((Long, String), Int)] = filteredUserVisitActionRDD.map(action => ((action.click_category_id, action.session_id), 1))
        // 同一个cid进入同一分区
        val cidAndSessionCount: RDD[(Long, (String, Int))] = cidSessionAndOne.reduceByKey(new CidPartitioner(cids.length, cids), _ + _).map {
            case ((cid, sessionId), count) => (cid, (sessionId, count))
        }
        val top10SessionRDD: RDD[CategorySession] = cidAndSessionCount.mapPartitions(it => {
            var set: mutable.TreeSet[CategorySession] = mutable.TreeSet[CategorySession]()
            it.foreach {
                case (cid, (sid, count)) =>
                    set += CategorySession(cid, sid, count)
                    if (set.size > 10) set = set.dropRight(1)
        
            }
            set.toIterator
        })
        top10SessionRDD.collect.foreach(println)
    }
    
}

class CidPartitioner(num: Int, cids: List[Long]) extends Partitioner {
    val map: Map[Long, Int] = cids.zipWithIndex.toMap
    override def numPartitions: Int = num
    
    override def getPartition(key: Any): Int = {
        key match {
                // 分区号, 不能取模. 可以使用cid在集合中的索引
            case (cid: Long, _) => map(cid); cids.indexOf()
        }
    }
}



/*
使用RDD的排序功能:
    优点:
        不会oom
    缺点:
        有多个品类id, 就需要多个job

既不oom, 又不用起多个 job 的?
    
    TreeMap 或 TreeSet 自动排序序功能
    
一个分区一次
    

 

 */