package com.atguigu.spark.core.project.util

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * Author lzc
  * Date 2019/12/13 11:39
  */
object JDBCUtil {
    def main(args: Array[String]): Unit = {
        
    }
    
    val url = "jdbc:mysql://hadoop102:3306/rdd"
    val userName = "root"
    val passWd = "aaaaaa"
    
    Class.forName("com.mysql.jdbc.Driver")
    val conn: Connection = DriverManager.getConnection(url, userName, passWd)
    
    // 单条插入
    def insertSingle[_](sql: String, arr: Array[_]): Unit = {
        
        if (arr != null && arr.length > 0) {
            val ps: PreparedStatement = conn.prepareStatement(sql)
            // insert into user values(?,?,?)
            for (index <- 1 to arr.length) {
                ps.setObject(index, arr(index - 1))
            }
            ps.execute()
            ps.close()
        }
        
        
    }
    
    // 批量
    def insertBatch[_](sql: String, args: Iterable[Array[_]]): Unit = {
        if (args == null) return
        
        conn.setAutoCommit(false) // 手动提交
        val ps: PreparedStatement = conn.prepareStatement(sql)
        args.foreach(arr => {
            arr.zipWithIndex.foreach {
                case (ele, index) =>
                    ps.setObject(index + 1, ele)
            }
            ps.addBatch()
        })
        ps.executeBatch()
        conn.commit()
    }
    
    
    def close(): Unit = conn.close()
}
