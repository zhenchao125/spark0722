package com.atuigu.core.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/10 8:28
  */
object AggDemo {
    def main(args: Array[String]): Unit = {
        // 平均值
        val conf: SparkConf = new SparkConf().setAppName("AggDemo").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd = sc.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)), 2)
        val res: RDD[(String, (Int, Int))] = rdd.combineByKey(
            x => (x, 1),
            {
                case ((sum:Int, count:Int), e:Int) => (sum + e, count + 1)
            },
            {
                case ((sum1:Int, count1:Int), (sum2:Int, count2:Int)) => (sum1 + sum2, count1 + count2)
            }
        )
       val res1 = res.mapValues{
            case (sum, count) => (sum, count, sum.toDouble / count)
        }
    
        res1.collect.foreach(println)
        sc.stop()
        
        
    }
}

/*
四个聚合算子总结:

reduceByKey
    combineByKeyWithClassTag[V]((v: V) => v, func, func, partitioner)

foldByKey
    combineByKeyWithClassTag[V]((v: V) => cleanedFunc(createZero(), v),
      cleanedFunc, cleanedFunc, partitioner)

aggregateByKey
    combineByKeyWithClassTag[U]((v: V) => cleanedSeqOp(createZero(), v),
      cleanedSeqOp, combOp, partitioner)

combineByKey
    combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners)(null)
    

 */
