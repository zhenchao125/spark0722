package com.atguigu.spark.core.project.acc

import com.atguigu.spark.core.project.bean.UserVisitAction
import org.apache.spark.util.AccumulatorV2

/*
add(类型)
IN: (cid, "click")
     (cid, "order")
     (cid, "pay")
     
IN: UserVisitAction

value() 类型
OUT:
    Map[(cid1, (100, 10, 2))]
    
    Map[(cid1, "click"), 10), ((cid1, order), 10)....)]
    
    List[CategroyCount]
 */
class CategoryAcc extends AccumulatorV2[UserVisitAction, Map[(Long, String), Long]] {
    // 最终的返回数据   Map[(cid1, "click"), 10), ((cid1, order), 10)....)]
    private var _map: Map[(Long, String), Long] = Map[(Long, String), Long]()
    
    // 判断是否零值
    override def isZero: Boolean = _map.isEmpty
    
    // copy累加器
    override def copy(): AccumulatorV2[UserVisitAction, Map[(Long, String), Long]] = {
        val acc = new CategoryAcc
        //        acc._map = this._map
        acc._map ++= this._map
        acc
    }
    
    
    // 重置累加器
    override def reset(): Unit = this._map = Map[(Long, String), Long]() // 不可变map, 重新赋值一个新的Map
    
    // 堆累加器进行累加 核心方法
    override def add(v: UserVisitAction): Unit = {
        // 先判断是否click
        if (v.click_category_id != -1) { // 这次是点击事件
            _map += (v.click_category_id, "click") -> (_map.getOrElse((v.click_category_id, "click"), 0L) + 1L)
        } else if (v.order_category_ids != "null") { // 这次是下单  一个单会涉及到多个品类id
            val cids: Array[Long] = v.order_category_ids.split(",").map(_.toLong)
            cids.foreach(cid => {
                _map += (cid, "order") -> (_map.getOrElse((cid, "order"), 0L) + 1L)
            })
        } else if (v.pay_category_ids != "null") { // 这次是支付行为
            val cids: Array[Long] = v.pay_category_ids.split(",").map(_.toLong)
            cids.foreach(cid => {
                _map += (cid, "pay") -> (_map.getOrElse((cid, "pay"), 0L) + 1L)
            })
        }
    }
    
    // 分区间的合并
    override def merge(other: AccumulatorV2[UserVisitAction, Map[(Long, String), Long]]) = {
        // map的合并
        this._map = other match {
            case o: CategoryAcc =>
                // 合并两个 Map   折叠 foldLeft
                this._map.foldLeft(o._map) {
                    case (map, ((cid, action), count)) =>
                        map + ((cid, action) -> (map.getOrElse((cid, action), 0L) + count))
                }
            
            case _ => throw new UnsupportedOperationException("不支持...")
        }
    }
    
    // 返回累加器最后累加的值
    override def value: Map[(Long, String), Long] = _map
}
