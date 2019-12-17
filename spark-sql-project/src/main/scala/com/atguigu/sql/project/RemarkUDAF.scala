package com.atguigu.sql.project

import java.text.DecimalFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.immutable.Nil

// 输入: 北京    聚合结果: 北京21.2%，天津13.2%，其他65.6%
class RemarkUDAF extends UserDefinedAggregateFunction {
    // 输入的数据类型
    override def inputSchema: StructType =
        StructType(StructField("city_name", StringType) :: Nil)
    
    /* 中间缓冲的数据类型
        北京  点击数
        天津  点击数
            ...
     */
    override def bufferSchema: StructType =
        StructType(StructField("map", MapType(StringType, LongType)) :: StructField("total", LongType) :: Nil)
    
    // 最终的结果的类型
    override def dataType: DataType = StringType
    
    // 确定
    override def deterministic: Boolean = true
    
    // 对缓冲进行初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        // 北京-> 1000
        buffer(0) = Map[String, Long]()
        // 总数
        buffer(1) = 0L
    }
    
    // 分区内聚合
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        if (!input.isNullAt(0)) { // 对传入的参数做非空判断
            
            val cityName: String = input.getString(0)
            val map: collection.Map[String, Long] = buffer.getMap[String, Long](0)
            // 更新城市计数
            buffer(0) = map + (cityName -> (map.getOrElse(cityName, 0L) + 1L))
            // 总数 + 1
            buffer(1) = buffer.getLong(1) + 1L
            
        }
    }
    
    // 分区间的聚合
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        val map1: collection.Map[String, Long] = buffer1.getMap[String, Long](0)
        val map2: collection.Map[String, Long] = buffer2.getMap[String, Long](0)
        
        
        // 更新buff1中的map
        buffer1(0) = map1.foldLeft(map2) {
            case (map, (city_name, count)) =>
                map + (city_name -> (map.getOrElse(city_name, 0L) + count))
        }
        
        // 更新总数
        buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }
    
    // 返回最终的聚合结果
    override def evaluate(buffer: Row): Any = {
        val map: collection.Map[String, Long] = buffer.getMap[String, Long](0)
        val total: Long = buffer.getLong(1)
        
        val cityCount: List[(String, Long)] = map.toList.sortBy(-_._2).take(2)
        val remarkTop2: List[Remark] = cityCount.map {
            case (city, count) => Remark(city, count.toDouble / total)
        }
        val resultList: List[Remark] = remarkTop2 :+ Remark("其他", 1 - remarkTop2.foldLeft(0D)(_ + _.rate))
        //        val resultList: List[Remark] = remarkTop2 :+ Remark("其他", 1 - remarkTop2.reduce((r1, r2) => Remark("其他", r1.rate + r2.rate)).rate)
        
        resultList.mkString(",")
    }
}

case class Remark(cityName: String, rate: Double) {
    private val f = new DecimalFormat(".00%")
    override def toString: String = s"$cityName: ${f.format(rate)}"
}
