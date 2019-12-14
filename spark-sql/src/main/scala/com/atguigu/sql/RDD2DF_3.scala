package com.atguigu.sql


import org.apache.spark.sql.types._
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


/*
RDD到df
    1. 手动转换: 手动指定df中的列名
        val df: DataFrame = rdd.toDF("age", "id")
    2. 使用样例类进行转换
        val df: DataFrame = usersRDD.toDF()
        case class User(age: Int, name: String)
    3. 代码api的方式:


 */