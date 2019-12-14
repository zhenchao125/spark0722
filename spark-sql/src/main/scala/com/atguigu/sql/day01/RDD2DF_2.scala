package com.atguigu.sql.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author lzc
  * Date 2019/12/14 15:02
  */
object RDD2DF_2 {
    def main(args: Array[String]): Unit = {
        // 入口
        val spark: SparkSession = SparkSession.builder()
            .master("local[*]")
            .appName("RDD2DF")
            .getOrCreate()
        import spark.implicits._
        val usersRDD: RDD[User] = spark.sparkContext.parallelize(Array(User(10, "lisi"), User(20, "zs")))
        val df: DataFrame = usersRDD.toDF()
        df.show
        
        spark.stop()
    }
}
case class User(age: Int, name: String)

/*
1. 手动转换: 手动指定df中的列名
    val df: DataFrame = rdd.toDF("age", "id")
2. 使用样例类进行转换
    val df: DataFrame = usersRDD.toDF()
    case class User(age: Int, name: String)
3. 代码的方式:

 */