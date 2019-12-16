package com.atguigu.sql.day02.udf

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Author lzc
  * Date 2019/12/16 8:58
  */
object MySumDemo {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("MySumDemo")
            .getOrCreate()
        val df: DataFrame = spark.read.json("c:/users.json")
        df.createOrReplaceTempView("user")
        spark.udf.register("mySum", new MySum)
        spark.sql("select mySum(age) from user").show
        spark.close()
        
        
    }
}


class MySum extends UserDefinedAggregateFunction {
    // 指定输入的数据的类型
    override def inputSchema: StructType = StructType(StructField("column", LongType) :: Nil)
    
    // 缓冲区的类型
    override def bufferSchema: StructType = StructType(StructField("sum", LongType) :: Nil)
    
    // 最终聚合值的数据类型
    override def dataType: DataType = LongType
    
    // 如果相同的输入是否应该有相同的输出
    override def deterministic: Boolean = true
    
    // 初始化缓冲区的值
    override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0) = 0L
    
    
    // 分区内聚合
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        // 从传入的数据中, 取出值, 更新buffer
        buffer(0) = buffer.getLong(0) + input.getLong(0)
    }
    // 分区间的聚合操作  (聚合后的结果应该更新到buffer1中)
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
//        buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
        buffer1(0) = buffer1.getLong(0) + buffer2.getAs[Long](0)
    }
    
    // 返回最后的聚合后的值
    override def evaluate(buffer: Row): Any = buffer(0)
}