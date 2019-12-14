package com.atguigu.sql.day01

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Author lzc
  * Date 2019/12/14 16:17
  */
object DSDF {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("DSDF")
            .getOrCreate()
        import spark.implicits._
        val df: DataFrame = spark.read.json("c:/users.json")
        val ds: Dataset[Person] = df.as[Person]
        
        ds.show
        
        val df1: DataFrame = ds.toDF()
        df1.show
        
        spark.close()
        
        
    }
}

case class Person(age: Long, name: String)
/*
df -> ds
    
    df.as[样例类]
ds -> df
    ds.toDF
 
 */