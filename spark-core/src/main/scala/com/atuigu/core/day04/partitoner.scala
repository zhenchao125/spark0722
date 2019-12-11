package com.atuigu.core.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/11 10:36
  */
object partitoner {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("partitoner").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        val rdd2: RDD[(Int, Int)] = rdd1.map((_, 1))
        val rdd3: RDD[(Int, Int)] = rdd2.reduceByKey(new MyPartitioner(4), _ + _)
        val rdd4: RDD[Array[(Int, Int)]] = rdd3.glom()
        rdd4.collect.foreach(arr => println(arr.length))
        sc.stop()
        
    }
}

class MyPartitioner(partitionNum : Int) extends Partitioner {
    override def numPartitions: Int = partitionNum
    
    override def getPartition(key: Any): Int = key.hashCode() % partitionNum abs
    
    
}
