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
    println(s1) // (AABCD,9)
    // 将 key 归约 , values 求和 reduceByKey(func()=>???)
    val rdd2 =  rdd1.reduceByKey((x,y)=>{x+y})

    rdd2.take(2) // 不排序 , 取 前 n 个
    rdd2.top(2) // 按照 降序 或者 指定规则排序 , 取 前 n 个
    rdd2.takeOrdered(2) // 按照 升序 排序 , 取 前 n 个

    val rdd3 = sc.makeRDD(1 to 10, 2)
    rdd3.mapPartitionsWithIndex((index,iter)=>{
      var p_map = scala.collection.mutable.Map[String,List[Int]]()
      while(iter.hasNext){
        var p_name = "p_" + index
        var it = iter.next()
        if(p_map.contains(p_name)){
          var elems = p_map(p_name)
          elems ::= it
          p_map(p_name) = elems
        } else {
          p_map(p_name) = List[Int](it)
        }
      }
      p_map.iterator
    }).collect().foreach(println)
    //(p_0,List(5, 4, 3, 2, 1))
    //(p_1,List(10, 9, 8, 7, 6))

    // 规约操作 就是 把每次计算的结果 作为 下次计算的值,有默认值的,先处理默认值
    val aggValue = rdd3.aggregate(0)(
      {(x:Int,y:Int) => x+y}, // SeqOp(U,T)=>U  先将 各个分区的 值求和 得到 // p0: 15 ,p1: 40
      {(x:Int,y:Int) => {
        println("x:"+x)
        println("y:"+y)
        x+y
      }}  // CombOp(U,U)=>U  再将 两个分区的 值 合并  o+15=15 15+40
    )
    println(aggValue) // 55

    // fold 为 aggregate的 简化版
    val foldValue = rdd3.fold(0)(
      {(x:Int,y:Int) => x+y} // op:(T,T)=>  T
    )
    println(foldValue) // 55

    // lookup(key) -> Seq(values)
    println(rdd1.lookup("A").toString()) // (2, 1)


  }

}
