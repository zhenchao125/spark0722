package com.atguigu.sql.day02.jdbc

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Author lzc
  * Date 2019/12/16 11:33
  */
object JDBCWrite {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("JDBCWrite")
            .getOrCreate()
        import spark.implicits._
        
        val rdd = spark.sparkContext.parallelize(Array(User(10, "lis"), User(20, "zx")))
        val df: DataFrame = rdd.toDF
        
        // 通用的
        /*df.write.format("jdbc")
//            .mode("append")
            .mode(SaveMode.Overwrite)
//            .option("truncate", "true")
            .option("url", "jdbc:mysql://hadoop102:3306/rdd")
            .option("user", "root")
            .option("password", "aaaaaa")
            .option("dbtable", "user1")
            .save()*/
    
        val url = "jdbc:mysql://hadoop102:3306/rdd"
        val table = "user1"
        val props: Properties = new Properties()
        props.setProperty("user", "root")
        props.setProperty("password", "aaaaaa")
        df.write.mode(SaveMode.Overwrite).jdbc(url, table, props)
        spark.close()
        
        
    }
}

case class User(age: Int, name: String)