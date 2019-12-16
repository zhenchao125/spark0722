package com.atguigu.sql.day02.jdbc

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author lzc
  * Date 2019/12/16 10:58
  */
object JDBCRead {
    
    
    
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("JDBCRead")
            .getOrCreate()
        import spark.implicits._
        
        /*// 通用读:
        val df: DataFrame = spark.read.format("jdbc")
            .option("url", "jdbc:mysql://hadoop102:3306/rdd")
            .option("user", "root")
            .option("password", "aaaaaa")
            .option("dbtable", "CategoryCountInfo")
//            .option("query", "select cid, click_count from CategoryCountInfo")
            .load()
        */
        
        
        // 专用的读
        val url = "jdbc:mysql://hadoop102:3306/rdd"
        val table = "CategoryCountInfo"
        val props: Properties = new Properties()
        props.setProperty("user", "root")
        props.setProperty("password", "aaaaaa")
        val df = spark.read.jdbc(url, table, props)
        
        df.show
        spark.close()
        
        
    }
}

/*
jdbc读

jdbc写


*/