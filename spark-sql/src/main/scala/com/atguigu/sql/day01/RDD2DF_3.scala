package com.atguigu.sql.day01

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Author lzc
  * Date 2019/12/14 15:02
  */
object RDD2DF_3 {
    def main(args: Array[String]): Unit = {
        // 入口
        val spark: SparkSession = SparkSession.builder()
            .master("local[*]")
            .appName("RDD2DF")
            .getOrCreate()
        val rdd = spark
            .sparkContext.parallelize(List(("abc", 1), ("aaa", 2)))
            .map {
                case (name, age) => Row(name, age)
            }
        
        //        val struct = StructType(Array(StructField("name", StringType), StructField("age", IntegerType)))
        val struct = StructType(StructField("name", StringType) :: StructField("age", IntegerType) :: Nil)
        
        val df = spark.createDataFrame(rdd, struct)
        df.show
        
        
        spark.stop()
    }
}
