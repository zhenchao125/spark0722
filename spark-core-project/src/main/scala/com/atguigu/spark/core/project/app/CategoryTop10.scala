package com.atguigu.spark.core.project.app

import com.atguigu.spark.core.project.acc.CategoryAcc
import com.atguigu.spark.core.project.bean.{CategoryCountInfo, UserVisitAction}
import com.atguigu.spark.core.project.util.JDBCUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Author lzc
  * Date 2019/12/13 9:39
  */
object CategoryTop10 {
    def statCategoryTop10(sc: SparkContext, userVisitActionRDD: RDD[UserVisitAction]): List[Long] = {
        // 遍历的方式去一次统计三个指标.
        // 点击 下单 支付
        val acc = new CategoryAcc
        sc.register(acc, "CategoryAcc")
        //        userVisitActionRDD.foreach(userVisitAction => acc.add(userVisitAction))
        userVisitActionRDD.foreach(acc.add)
        
        // Map[(Long, String), Long] => Map[(cid, "click"), 10000]
        val categoryCountGrouped: Map[Long, Map[(Long, String), Long]] = acc.value.groupBy(_._1._1)
        
        // 把每个品类的3个指标封装到一个对象中
        val categoryCountInfoList: List[CategoryCountInfo] = categoryCountGrouped.map {
            case (cid, map) =>
                CategoryCountInfo(cid,
                    map.getOrElse((cid, "click"), 0),
                    map.getOrElse((cid, "order"), 0),
                    map.getOrElse((cid, "pay"), 0))
        }.toList
        
        val result: List[CategoryCountInfo] = categoryCountInfoList.sortBy(info => (-info.clickCount, -info.orderCount, -info.payCount)).take(10)
        //        result.foreach(println)
        val sql = "insert into CategoryCountInfo values(?, ?, ?, ?)"
        /*result.foreach(info => {
            JDBCUtil.insertSingle(sql, Array(info.categoryId, info.clickCount, info.orderCount, info.payCount))
        })*/
        JDBCUtil.insertBatch(sql, result.map(info => Array(info.categoryId, info.clickCount, info.orderCount, info.payCount)))
        JDBCUtil.close()
        
        result.map(_.categoryId)
    }
}

/*

=> List[(cid, clickCount), (cid, orderCount)] groupby
=> Map[(cid, it[(cid, cliclCount), (...), ()] map
=> List[(cid, (clickCount, orderCount, payCount))]   作废
=> LIst(CategoryCount(cid, clickCount,...))

 */