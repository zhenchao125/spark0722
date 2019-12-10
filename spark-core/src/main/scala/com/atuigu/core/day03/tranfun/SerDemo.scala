package com.atuigu.core.day03.tranfun

import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.spark.rdd.RDD

/**
  * Author lzc
  * Date 2019/12/10 15:14
  */
object SerDemo {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf()
            .setAppName("SerDemo")
            .setMaster("local[*]")
            .registerKryoClasses(Array(classOf[Searcher]))
        
        
        val sc = new SparkContext(conf)
        val rdd: RDD[String] = sc.parallelize(Array("hello world", "hello atguigu", "atguigu", "hahah"), 2)
        val searcher = new Searcher("hello")
        val resRDD = searcher.search(rdd)
        
        resRDD.collect.foreach(println)
    }
}
//需求: 在 RDD 中查找出来包含 query 子字符串的元素
class Searcher(val query: String) extends Serializable {
    def isMatch(x: String): Boolean = {
        val q = query
        x.contains(q)
    }
    
    def search(rdd: RDD[String]): RDD[String] = {
        rdd.filter(x => x.contains(query))
//        rdd.filter(this.isMatch)
    }
    def search1(rdd: RDD[String]): RDD[String] = {
        val q = query
        rdd.filter(x => x.contains(q))
        
    }
    
}
/*
1. 让类实现 Serializable
2. 把属性使用局部变量来保存, 然后传递局部遍历
 */
