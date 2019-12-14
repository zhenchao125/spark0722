package com.atguigu.sql.day01

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author lzc
  * Date 2019/12/14 15:31
  */
object DF2RDD {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("DF2RDD")
            .getOrCreate()
        
        val df: DataFrame = spark.read.json("c:/users.json")
        df.printSchema()
        val rdd= df.rdd.map(row => {
            row.getLong(0)
        })
        rdd.collect().foreach(println)
        spark.close()
    }
}
