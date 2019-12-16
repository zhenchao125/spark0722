package com.atguigu.sql.day02.hive

import org.apache.spark.sql.SparkSession

/**
  * Author lzc
  * Date 2019/12/16 15:05
  */
object HiveDemo1 {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "atguigu")
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("HiveDemo1")
            .enableHiveSupport() // 支持hive
            .config("spark.sql.warehouse.dir", "hdfs://hadoop102:9000/user/hive/warehouse")
            .getOrCreate()
        //        spark.sql("show databases").show
        //        spark.sql("create table abc(id int)").show
        //        spark.sql("use spark0722")
        //        spark.sql("select * from a").show
        
//        spark.sql("create database bbb").show
        
        spark.sql("use gmall")
        spark.sql("select count(*) from ads_uv_count").show
        
        
        spark.close()
        
        
    }
}
