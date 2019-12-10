package com.atuigu.core.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/10 9:58
  */
object SorkByKey {
   /* implicit val ord = new Ordering[User]{
        override def compare(x: User, y: User): Int = x.age - y.age
    }
    */
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("SorkByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
//        val rdd = sc.parallelize(Array((1, "a"), (10, "b"), (11, "c"), (4, "d"), (20, "d"), (10, "e")))
//        val res: RDD[(Int, String)] = rdd.sortByKey(ascending = false, numPartitions = 10)
        val rdd = sc.parallelize(Array(User(10, "a"), User(8, "c"), User(12, "b"))).map((_, 1))
        val res: RDD[(User, Int)] = rdd.sortByKey(ascending = false)
        
        res.collect.foreach(println)
        sc.stop()
        
    }
}
case class User(age: Int, name:String) extends Ordered[User] {
    override def compare(that: User): Int = this.age - that.age
}
