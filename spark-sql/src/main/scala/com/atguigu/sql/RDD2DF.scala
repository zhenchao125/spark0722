package com.atguigu.sql

import org.apache
import org.apache.spark
import org.apache.spark.rdd.RDD
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
/*
1. 手动转换: 手动指定df中的列名
    val df: DataFrame = rdd.toDF("age", "id")
2. 使用样例类进行转换

 */