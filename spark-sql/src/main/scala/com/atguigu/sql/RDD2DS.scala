package com.atguigu.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Author lzc
  * Date 2019/12/14 16:02
  */
object RDD2DS {
    def main(args: Array[String]): Unit = {
        // 入口
        val spark: SparkSession = SparkSession.builder()
            .master("local[*]")
            .appName("RDD2DF")
            .getOrCreate()
        import spark.implicits._
        val usersRDD: RDD[User] = spark.sparkContext.parallelize(Array(User(10, "lisi"), User(20, "zs")))
        val ds: Dataset[User] = usersRDD.toDS()
        ds.show()
        ds.rdd.collect().foreach(println)
        spark.close()
        
    }
}
/*



RDD->DS(DF)
   现有样例类, 然后在RDD中存储样例类
   rdd.toDS   (rdd.toDF)
DS(DF) -> RDD
    ds.rdd
    (df.rdd)
 
 
 */