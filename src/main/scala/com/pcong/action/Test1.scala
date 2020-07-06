package com.pcong.action

import com.pcong.base.SparkBaseSession

object Test1 {

  def main(args: Array[String]): Unit = {
    val spark =  SparkBaseSession.newSparkSession()
    val rdd1 =  spark.sparkContext.makeRDD( 1 to 5 ,5)
    /**
     *  map : e => e
     *  mapPartitions : e => e.iterator , (2) preservesPartitioning 是否保留父rdd 的 分区器的信息
     */
    val rdd1_1 = rdd1.mapPartitions(x => {
      var res = List[Int]()
      var i = 0 ;
      while(x.hasNext){
        i += x.next()
      }
      res.::(i).iterator
    })
    rdd1_1.collect().foreach(e=>println(e))

    /**
     *  与 mapPartitions 类似 只是多了一个分区索引信息
     */
    val rdd1_2  = rdd1.mapPartitionsWithIndex((x , iter) => {
      var res = List[String]()
      var i = 0 ;
      while(iter.hasNext){
        i += iter.next()
      }
      res.::("("+ x +","+ i + ")").iterator
    })

    rdd1_2.collect().foreach(e => println(e))
//    (0,1)
//    (1,2)
//    (2,3)
//    (3,4)
//    (4,5)


  }

}
