package com.atguigu.spark.core.project.app

import java.text.DecimalFormat

import com.atguigu.spark.core.project.bean.UserVisitAction
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Author lzc
  * Date 2019/12/14 8:56
  */
object PageConversionApp {
    
    def statPageConversionRate(sc: SparkContext, userVisitActionRDD: RDD[UserVisitAction], pagesString: String): Unit = {
        val pages: Array[String] = pagesString.split(",")
        val prePages: Array[String] = pages.slice(0, pages.length - 1)
        val postPage: Array[String] = pages.slice(1, pages.length)
        //0.目标跳转流
        val targetPageFlow: Array[String] = prePages.zip(postPage).map {
            case (pre, post) => s"$pre->$post"
        }
        
        
        // 1. 计算需求页面的点击数  (最后一个页面不需要计算)
        val pageCountMap: collection.Map[Long, Long] = userVisitActionRDD
            .filter(action => prePages.contains(action.page_id.toString))
            .map(action => (action.page_id, 1))
            .countByKey()
        
        
        // 2. 从一个页面到另外一个页面的跳转次数   (必须按照session进行分组)
        val actionGroupedRDD: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD.groupBy(_.session_id)
        // RDD["1->2", "2->3"]
        val allTargetFlowRDD: RDD[String] = actionGroupedRDD.flatMap {
            case (_, it) =>
                // 先按照时间戳进行升序排列
                val actionSortedList: List[UserVisitAction] = it.toList.sortBy(_.action_time)
                // 计算跳转流
                val preAction: List[UserVisitAction] = actionSortedList.slice(0, actionSortedList.length - 1)
                val postAction: List[UserVisitAction] = actionSortedList.slice(1, actionSortedList.length)
                val allPageFlow: List[String] = preAction.zip(postAction).map {
                    case (pre, post) => s"${pre.page_id}->${post.page_id}"
                }
                // 过滤出来所有的目标跳转流
                allPageFlow.filter(flow => targetPageFlow.contains(flow))
        }
        val pageFlowCount: RDD[(String, Int)] = allTargetFlowRDD.map((_, 1)).reduceByKey(_ + _)
       
        // 3. 跳转率
        val formatter = new DecimalFormat(".00%")
        val pageFlowRate: RDD[(String, String)] = pageFlowCount.map {
            case (flow, flowCount) =>
                // "2->3"
                val preCount: Long = pageCountMap.getOrElse(flow.split("->")(0).toLong, 0)
                val flowRate = formatter.format(flowCount.toDouble / preCount)
                (flow, flowRate)
        }
        
        pageFlowRate.collect.foreach(println)
        
    }
    
}

/*
页面单调转化率

"1,2,3,4,5,6,7"

计算1-2跳转率
1. 打开页面1的次数  c1
2. "1->2" 这个跳转的流程多少  c2 按照session分组
3. c2/c1


reduceByKey
countByKey

 */