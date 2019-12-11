package com.atuigu.core.day04.add

import org.apache.spark.util.AccumulatorV2

class IntAcc extends AccumulatorV2[Int, Long] {
    
    private var _sum: Long = 0
    
    // 判断是否为 0
    override def isZero: Boolean = _sum == 0
    
    // copy一个累加器
    override def copy(): AccumulatorV2[Int, Long] = {
        val acc: IntAcc = new IntAcc
        acc._sum = this._sum
        acc
    }
    
    // 重置累加器
    override def reset(): Unit = this._sum = 0
    
    // 核心方法: 累加
    override def add(v: Int): Unit = _sum += v
    
    // 分区间的合并 是把other的值, 合并到this中
    override def merge(other: AccumulatorV2[Int, Long]): Unit = {
        // 2同, 2小, 1大
        other match {
            case o: IntAcc => this._sum += o._sum
            case _ =>
        }
    }
    
    // 累加后的值
    override def value: Long = this._sum
}
