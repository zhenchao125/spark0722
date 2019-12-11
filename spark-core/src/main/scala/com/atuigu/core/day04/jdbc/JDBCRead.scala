package com.atuigu.core.day04.jdbc

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/11 14:07
  */
object JDBCRead {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("JDBCRead").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
    
        val url = "jdbc:mysql://hadoop102:3306/rdd"
        val userName = "root"
        val passWd = "aaaaaa"
    
        val rdd = new JdbcRDD[(Int, String)](
            sc,
            () => {
                Class.forName("com.mysql.jdbc.Driver")
                DriverManager.getConnection(url, userName, passWd)
            },
            "select id, name from user where id >= ? and id <= ?",
            10,
            20,
            2,
            resultSet => {
                (resultSet.getInt(1), resultSet.getString(2))
            }
        )
    
        rdd.collect.foreach(println)
        
        sc.stop()
        
    }
}

/*


*/