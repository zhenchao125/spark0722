package com.atuigu.core.day04.hbas


import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/11 14:40
  */
object HbaseRead {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("HbaseRead").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        
        val hbaseConf: Configuration = HBaseConfiguration.create()
        hbaseConf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104")
        hbaseConf.set(TableInputFormat.INPUT_TABLE, "student")
        
        
        val rdd: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
            hbaseConf,
            classOf[TableInputFormat],
            classOf[ImmutableBytesWritable],
            classOf[Result]
        )
        
        val rdd1 = rdd.map {
            case (a, result) => /*Bytes.toString(a.get())*/
                //                Bytes.toString(result.getRow)
                var map: Map[String, String] = Map[String, String]()
                val cells: util.List[Cell] = result.listCells()
                //
                import scala.collection.JavaConversions._
                for (cell <- cells) {
                    map += Bytes.toString(CellUtil.cloneQualifier(cell)) -> Bytes.toString(CellUtil.cloneValue(cell))
                }
                map
        }
        rdd1.collect.foreach(println)
        
        sc.stop()
        
    }
}
