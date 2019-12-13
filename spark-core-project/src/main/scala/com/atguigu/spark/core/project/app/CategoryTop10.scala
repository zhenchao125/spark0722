package com.atguigu.spark.core.project.app

import com.atguigu.spark.core.project.acc.CategoryAcc
import com.atguigu.spark.core.project.bean.UserVisitAction
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Author lzc
  * Date 2019/12/13 9:39
  */
object CategoryTop10 {
    def statCategoryTop10(sc: SparkContext, userVisitActionRDD: RDD[UserVisitAction]) = {
        // 遍历的方式去一次统计三个指标.
        // 点击 下单 支付
        val acc = new CategoryAcc
        sc.register(acc, "CategoryAcc")
//        userVisitActionRDD.foreach(userVisitAction => acc.add(userVisitAction))
        userVisitActionRDD.foreach(acc.add)
    
        println(acc.value)
        
    }
}
