package com.atguigu.sql.day01

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Author lzc
  * Date 2019/12/14 16:31
  */
object FDDSDemo1 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("FDDSDemo1")
            .getOrCreate()
        import spark.implicits._
        val df: DataFrame = spark.read.json("c:/users.json")
        df.filter($"age" > 10).show()
//        val ds: Dataset[Long] = df.map(row => row.getLong(0))
//        ds.show()
        
        /*val ds: Dataset[Person] = df.as[Person]
        val ds1: Dataset[Long] = ds.map(p => p.age)
        ds1.show()*/
        
//        df.select("age").show()
    
        val ds: Dataset[Person] = df.as[Person]
//        ds.select("name").show()
        ds.filter(p => p.age > 10).show
        ds.filter($"age" > 10).show
        spark.close()
    }
}
