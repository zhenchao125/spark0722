package com.atguigu.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author lzc
  * Date 2019/12/17 15:06
  */
object MyReceiverDemo {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyReceiverDemo")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
        
        val receiverStream: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver("hadoop102", 10000))
        val resultDSteam: DStream[(String, Int)] = receiverStream
            .flatMap(line => line.split("\\W+"))
            .map((_, 1))
            .reduceByKey(_ + _)
        
        resultDSteam.print(100)
        
        ssc.start()
        ssc.awaitTermination()
        
        
    }
}


class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    var socket: Socket = null
    var reader: BufferedReader = null
    
    override def onStart(): Unit = {
        /*
        如何从 socket 读数据?
        1. 创建一个socket 对象
        2. 从socket中获取一个输入流(字节流)
        3. 封装成的字符流
         */
        
        runInthread {
            try {
                socket = new Socket(host, port)
                reader = new BufferedReader(new InputStreamReader(socket.getInputStream, "utf-8"))
                var line: String = reader.readLine()
                while (socket.isConnected && line != null) {
                    store(line)
                    line = reader.readLine()
                }
                
            } catch {
                case _ =>
            } finally {
                restart("重启接收器..." + Thread.currentThread().getName)
            }
        }
    }
    
    def runInthread(op: => Unit): Unit = {
        new Thread() {
            override def run(): Unit = op
        }.start()
    }
    
    override def onStop(): Unit = {
        reader.close()
        socket.close()
    }
}