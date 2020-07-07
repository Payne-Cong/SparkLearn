package com.pcong.action

import com.pcong.base.SparkBaseSession

object Test2 {

  def main(args: Array[String]): Unit = {
    val spark =  SparkBaseSession.newSparkSession()
    val sc = spark.sparkContext

    val rdd1 = sc.makeRDD(Array(("A",2),("A",1),("B",3),("C",1),("D",2)))

    val s1 = rdd1.reduce((x,y)=>{
      (x._1+y._1 , x._2+y._2)
    })
    println(s1)
    // 将 key 归约 , values 求和 reduceByKey(func()=>???)
    val rdd2 =  rdd1.reduceByKey((x,y)=>{x+y})

    rdd2.take(2) // 不排序 , 取 前 n 个
    rdd2.top(2) // 按照 降序 或者 指定规则排序 , 取 前 n 个
    rdd2.takeOrdered(2) // 按照 升序 排序 , 取 前 n 个



  }

}
