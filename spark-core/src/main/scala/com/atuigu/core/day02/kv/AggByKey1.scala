package com.atuigu.core.day02.kv

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/9 15:21
  */
object AggByKey1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("AggByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
        /*val res = rdd.aggregateByKey((Int.MinValue, Int.MaxValue))(
            (kv, e) => (kv._1.max(e), kv._2.min(e)),
            (kv1, kv2) => (kv1._1 + kv2._1, kv1._2 + kv2._2))*/
        val res = rdd.aggregateByKey((Int.MinValue, Int.MaxValue))(
            {
                case ((max, min), e) => (max.max(e), min.min(e))
            },
            {
                case ((max1, min1), (max2, min2)) => (max1 + max2, min1 + min2)
            }
        )
        
        // 计算出来每个key对应的值的平均值!!
        res.collect.foreach(println)
        sc.stop()
        
    }
}

/*
reduceByKey(f)
foldByKey(zero)(f)
aggregateByKey(zero)(f1, f2)
    zero使用最多和分区数保持一致
    

    

 */