package com.atguigu.sql.day02.udf

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

/**
  * Author lzc
  * Date 2019/12/16 8:58
  */
object MySumDemo2 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("MySumDemo")
            .getOrCreate()
        import spark.implicits._
        val df: DataFrame = spark.read.json("c:/users.json").selectExpr("cast(age as integer)", "name")
        val ds: Dataset[User] = df.as[User]
//        val my_avg: TypedColumn[User, Double] = new MyAvg().toColumn.name("my_avg")
        val my_avg: TypedColumn[User, Double] = MyAvg.toColumn.name("my_avg")
        val ds1: Dataset[Double] = ds.select(my_avg)
        ds1.show()
        spark.close()
        
    }
}

/* [-IN, BUF, OUT]
-IN, Datset 强类型   K的泛型是指定DS中存储的数据的类型(样例类)
BUF sum, count 用样例类来缓存sum和count, 就是样例类的类型
OUT 最终的聚合后的值



 */
// DS存储的数据的类型
case class User(age: Int, name: String)
// 缓冲类型
case class Avg(var sum: Long, var count: Long)

object MyAvg extends Aggregator[User, Avg, Double]{
    // 对缓冲做初始化
    override def zero: Avg = Avg(0, 0)
    
    // 分区内的聚合
    override def reduce(b: Avg, a: User): Avg = {
        b.sum += a.age
        b.count += 1
        b
    }
    
    // 分区间的聚合  (聚合到b1中)
    override def merge(b1: Avg, b2: Avg): Avg = {
        b1.sum += b2.sum
        b1.count += b2.count
        b1
    }
    
    // 返回最终聚合后的值
    override def finish(reduction: Avg): Double = reduction.sum.toDouble / reduction.count
    
    // 对缓冲区的编码器
    override def bufferEncoder: Encoder[Avg] = Encoders.product
    
    // 输出的编码器
    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}


