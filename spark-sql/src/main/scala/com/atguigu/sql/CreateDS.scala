package com.atguigu.sql

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Author lzc
  * Date 2019/12/14 15:41
  */
object CreateDS {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("CreateDS")
            .getOrCreate()
        import spark.implicits._
        
//        val users: Seq[User1] = Seq(User1(10, "a"), User1(20, "b"))
//        val ds: Dataset[User1] = users.toDS()
        val seq: Seq[Int] = Seq(1,2,3,4)
        val ds: Dataset[Int] = seq.toDS()
        ds.show()
        
        spark.close()
    }
}

case class User1(age: Int, name: String)
