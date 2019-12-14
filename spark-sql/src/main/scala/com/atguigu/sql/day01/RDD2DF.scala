package com.atguigu.sql.day01

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author lzc
  * Date 2019/12/14 15:02
  */
object RDD2DF {
    def main(args: Array[String]): Unit = {
        // 入口
        val spark: SparkSession = SparkSession.builder()
            .master("local[*]")
            .appName("RDD2DF")
            .getOrCreate()
        import spark.implicits._
        
//        val list1 = List(30, 50, 70, 60, 10, 20)
        val list1 = List((30, 1), (50, 2))
        val rdd = spark.sparkContext.parallelize(list1)
//        val df: DataFrame = rdd.toDF
        val df: DataFrame = rdd.toDF("age", "id")
        df.show()
        
        spark.stop()
    }
}
