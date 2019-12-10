package com.atuigu.core.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/10 10:35
  */
object Practice {
    def main(args: Array[String]): Unit = {
        /*
        => RDD[((省份, 广告)] map
        => RDD[((省份, 广告), 1)] reduceByKey
        => RDD[(省份, 广告), count))] map
        => RDD[(省份, (广告, count))]  groupBy
        => RDD[(省份, List((广告1, count), (广告2, count), (广告3, count), .....))]  => map (排序取3)
        => RDD[(省份, List(广告1, 广告2, 广告3))]
         */
        val conf: SparkConf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        // 1. 先读文件
        val sourceRDD: RDD[String] = sc.textFile(ClassLoader.getSystemResource("agent.log").getPath)
        // 2.  切割 RDD[((省份, 广告)]  RDD[((省份, 广告), 1)]
        val proAdsAndOneRDD: RDD[((String, String), Int)] = sourceRDD.map(line => {
            val arr: Array[String] = line.split(" ")
            ((arr(1), arr(4)), 1)
        })
        // 3. 聚合  RDD[(省份, 广告), count))] RDD[(省份, (广告, count))]
        val proAdsAndCount: RDD[(String, (String, Int))] = proAdsAndOneRDD
            .reduceByKey(_ + _)
            .map {
                case ((pro, ads), count) => (pro, (ads, count))
            }
        // 4. 按照key分组   RDD[(省份, List((广告1, count), (广告2, count), (广告3, count), .....))]
        val proAndAdsCountGroupedRDD: RDD[(String, Iterable[(String, Int)])] = proAdsAndCount.groupByKey
        
        // 5. 排序取前三  RDD[(省份, List(广告1, 广告2, 广告3))]
        val res: RDD[(String, List[(String, Int)])] = proAndAdsCountGroupedRDD.map{
            case (pro, adsCountIt) => (pro, adsCountIt.toList.sortBy(-_._2).take(3))
        }/*.sortBy(_._1.toInt)*/
        res.collect.foreach(println)
        sc.stop()
        
    }
}

/*
1.	数据结构：时间戳，省份，城市，用户，广告，字段使用空格分割。
    1516609143867 6 7 64 16
    1516609143869 9 4 75 18
    1516609143869 1 7 87 12
2.	需求: 统计出每一个省份广告被点击次数的 TOP3


倒推法:
=> RDD[((省份, 广告)] map
=> RDD[((省份, 广告), 1)] reduceByKey
=> RDD[(省份, 广告), count))] map
=> RDD[(省份, (广告, count))]  groupBy
=> RDD[(省份, List((广告1, count), (广告2, count), (广告3, count), .....))]  => map (排序取3)
=> RDD[(省份, List(广告1, 广告2, 广告3))]

*/
