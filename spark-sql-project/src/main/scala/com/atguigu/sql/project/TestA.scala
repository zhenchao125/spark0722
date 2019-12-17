package com.atguigu.sql.project

import org.apache.spark.sql.SparkSession

/**
  * Author lzc
  * Date 2019/12/17 10:53
  */
object TestA {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Test")
            .enableHiveSupport()
            .getOrCreate()
        import spark.implicits._
        
        spark.sql("use sparkpractice")
        println(spark.sql("select * from city_count").rdd.getNumPartitions)
        
        spark.close()
        
        
    }
}
