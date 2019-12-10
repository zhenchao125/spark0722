package com.atuigu.core.day03

import scala.reflect.ClassTag

/**
  * Author lzc
  * Date 2019/12/10 9:21
  */
object ClassTagDemo {
    def main(args: Array[String]): Unit = {
        newArr[Int](10)
    }
    
    /*def newArr[T](len: Int)(implicit ct: ClassTag[T]) = {
        
        new Array[T](len)
    }*/
    
    def newArr[T: ClassTag](len: Int) = {
        new Array[T](len)
        val value: ClassTag[T] = implicitly[ClassTag[T]]
        println(value)
    }
}
