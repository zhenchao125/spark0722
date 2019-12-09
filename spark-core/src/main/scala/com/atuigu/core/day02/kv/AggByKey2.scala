package com.atuigu.core.day02.kv

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/9 15:21
  */
object AggByKey2 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("AggByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
        val res = rdd.aggregateByKey((0, 0))(
            {
                case ((sum, count), e) => (sum + e, count + 1)
            },
            {
                case ((sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2)
            }
        ) /*.map{
            case (key, (sum, count)) => (key, (sum, sum.toDouble / count))
        }*/
            .mapValues {
            case (sum, count) => (sum, sum.toDouble / count)
        }
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