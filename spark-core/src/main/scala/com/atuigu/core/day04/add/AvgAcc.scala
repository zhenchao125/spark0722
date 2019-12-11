package com.atuigu.core.day04.add

import org.apache.spark.util.AccumulatorV2

/**
  * Author lzc
  * Date 2019/12/11 16:35
  */
class AvgAcc extends AccumulatorV2[Double, Double] {
    private var _sum = 0D
    private var _count = 0L
    
    
    override def isZero: Boolean = _sum == 0D && _count == 0L
    
    override def copy(): AccumulatorV2[Double, Double] = {
        val acc = new AvgAcc
        acc._sum = this._sum
        acc._count = this._count
        acc
    }
    
    override def reset(): Unit = {
        this._sum = 0D
        this._count = 0L
    }
    
    override def add(v: Double): Unit = {
        this._sum += v
        this._count += 1
    }
    
    override def merge(other: AccumulatorV2[Double, Double]): Unit = other match {
        case o: AvgAcc =>
            this._sum += o._sum
            this._count += o._count
        case _ =>
            throw new UnsupportedOperationException("你传入的....")
    }
    
    override def value: Double = _sum/_count
}
