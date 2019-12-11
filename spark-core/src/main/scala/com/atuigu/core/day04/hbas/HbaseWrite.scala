package com.atuigu.core.day04.hbas

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author lzc
  * Date 2019/12/11 15:30
  */
object HbaseWrite {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("HbaseWrite").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        
        val hbaseConf = HBaseConfiguration.create()
        hbaseConf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104")
        hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "student")
        val job: Job = Job.getInstance(hbaseConf)
        job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
        job.setOutputKeyClass(classOf[ImmutableBytesWritable])
        job.setOutputValueClass(classOf[Put])
        
        
        val initialRDD: RDD[(String, String, String)] = sc.parallelize(List(("2000", "apple", "11"), ("3000", "banana", "12"), ("4000", "pear", "13")))
        val lastRDD: RDD[(ImmutableBytesWritable, Put)] = initialRDD.map {
            case (rowKey, col, value) =>
                val key = new ImmutableBytesWritable()
                key.set(Bytes.toBytes(rowKey))
                val put: Put = new Put(Bytes.toBytes(rowKey))
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(col), Bytes.toBytes(value))
                (key, put)
        }
        lastRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)
        sc.stop()
        
    }
}

/*
 rowKey Put


 */