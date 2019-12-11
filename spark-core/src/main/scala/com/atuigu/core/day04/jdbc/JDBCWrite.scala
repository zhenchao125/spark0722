package com.atuigu.core.day04.jdbc

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/11 14:23
  */
object JDBCWrite {
    def main(args: Array[String]): Unit = {
        
        val url = "jdbc:mysql://hadoop102:3306/rdd"
        val userName = "root"
        val passWd = "aaaaaa"
        
        val conf: SparkConf = new SparkConf().setAppName("JDBCWrite").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd: RDD[(Int, String)] = sc.parallelize(Array((100, "aa"), (101, "bb"), (102, "cc")))
        rdd.foreachPartition(it => {
            Class.forName("com.mysql.jdbc.Driver")
            val conn: Connection = DriverManager.getConnection(url, userName, passWd)
            it.foreach {
                case (id, name) => {
                    val ps: PreparedStatement = conn.prepareStatement("insert into user values(?, ?)")
                    ps.setInt(1, id)
                    ps.setString(2, name)
                    ps.execute()
                    ps.close()
                }
            }
            
            conn.close()
            
        })
        sc.stop()
        
    }
}
