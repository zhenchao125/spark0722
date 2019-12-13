package com.atguigu.spark.core.project.app

import com.atguigu.spark.core.project.bean.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/13 9:28
  */
object ProjectApp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("ProjectApp").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val userVisitActionRDD: RDD[UserVisitAction] = readFromFile(sc, "c:/user_visit_action.txt")
        
        // 需求1:
        val cidsTop10 = CategoryTop10.statCategoryTop10(sc, userVisitActionRDD)
        
        // 需求2:
        println("1-------")
        CategoryTop10Session.calcTop10Session(sc, cidsTop10, userVisitActionRDD)
        println("2-------")
        CategoryTop10Session.calcTop10Session_1(sc, cidsTop10, userVisitActionRDD)
        println("3-------")
        CategoryTop10Session.calcTop10Session_2(sc, cidsTop10, userVisitActionRDD)
        println("4-------")
        CategoryTop10Session.calcTop10Session_3(sc, cidsTop10, userVisitActionRDD)
        sc.stop()
        
        
    }
    
    
    /**
      * 从文件读取用户行为记录, 并封装到样例类中
      * @param sc
      * @param path
      * @return
      */
    def readFromFile(sc: SparkContext, path: String): RDD[UserVisitAction] = {
        val sourceRDD: RDD[String] = sc.textFile(path)
        sourceRDD.map(line => {
            val splits: Array[String] = line.split("_")
            UserVisitAction(
                splits(0),
                splits(1).toLong,
                splits(2),
                splits(3).toLong,
                splits(4),
                splits(5),
                splits(6).toLong,
                splits(7).toLong,
                splits(8),
                splits(9),
                splits(10),
                splits(11),
                splits(12).toLong)
        })
    }
}

/*
1. 先把文件读入

2. 把数据进行封装到样例中

3. 处理数据: 需求:  热门品类的top10
 */