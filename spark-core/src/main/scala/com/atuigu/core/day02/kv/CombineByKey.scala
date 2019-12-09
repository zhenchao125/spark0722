package com.atuigu.core.day02.kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/9 16:40
  */
object CombineByKey {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("CombineByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
        /*val res = rdd.combineByKey(
            x => x,
            (last: Int, e: Int) => last + e,
            (sum1: Int, sum2: Int) => sum1 + sum2
        )*/
        /*val res = rdd.combineByKey(
            x => x,
            (max: Int, e) => max.max(e),
            (max1: Int, max2:Int) => max1 + max2
        )
        */
        val res: RDD[(String, Int)] = rdd.combineByKey(
            x => x,
            {
                case (max: Int, e: Int) => max.max(e)
            },
            {
                case (max1: Int, max2: Int) => max1 + max2
            }
        )
        res.collect.foreach(println)
        sc.stop()
        
    }
}
