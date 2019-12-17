package com.atguigu.sql.project

import org.apache.spark.sql.SparkSession

/**
  * Author lzc
  * Date 2019/12/16 16:29
  */
object SqlApp {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "atguigu")
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("SqlApp")
            .enableHiveSupport()
            .getOrCreate()
        import spark.implicits._
        spark.sparkContext.setLogLevel("error")
        spark.sql("use sparkpractice")
        
        spark.sql(
            """
              |select
              |    ci.*,
              |    uva.click_product_id,
              |    pi.product_name
              |from user_visit_action uva
              |join city_info ci  on uva.city_id=ci.city_id
              |join product_info pi on uva.click_product_id=pi.product_id
            """.stripMargin).createOrReplaceTempView("t1")
        
        
        spark.udf.register("remark", new RemarkUDAF)
        spark.sql(
            """
              |select
              |    area,
              |    product_name,
              |    count(*) ct,
              |    remark(t1.city_name) remark
              |from t1
              |group by area, product_name
            """.stripMargin).createOrReplaceTempView("t2")
        
        spark.sql(
            """
              |select
              |    area,
              |    product_name,
              |    ct,
              |    remark,
              |    rank() over(partition by area order by ct desc) rk
              |from t2
            """.stripMargin).createOrReplaceTempView("t3")
        
        spark.sql(
            """
              |select
              |    area,
              |    product_name,
              |    ct,
              |    remark
              |from t3
              |where rk <= 3
            """.stripMargin).show(100, false)
        
        
        
        spark.close()
        
        
        
        
        
        //1. 读数据
        
        //2. 显示
    }
}
/*
各区域热门商品 Top3

1. 先把需要的字段查出来  (join)
// t1
select
    ci.*,
    uva.click_product_id,
    pi.product_name
from user_visit_action uva
join city_info ci  on uva.city_id=ci.city_id
join product_info pi on uva.click_product_id=pi.product_id

// t2
select
    area,
    product_name,
    count(*) ct
from t1
group by area, product_name

// t3
select
    area,
    product_name,
    ct,
    rank() over(partition by area order by ct desc) rk
from t2

// t4
select
    area,
    product_name,
    ct
from t3
where rk <= 3

 */