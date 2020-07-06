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

    val sc = spark.sparkContext

    val rdd2 = sc.makeRDD(1 to 5 ,2)
    val rdd3 = sc.makeRDD(Seq("A","B","C","D","E"),2)

    /**
     * zip : 将两个rdd 组合成 key/value 形式的 rdd ,要求 分区数 和 元素的 数量 完全一直
     * zipPartition :  要求分区数 一致
     *
     * zipIndex : 将 rdd 分区的 元素 和 索引号 组合成 键值对
     * zipWithUniqueId : 将 rdd 分区的 元素 和 一个唯一ID 组合成 键值对
     * 唯一ID 算法如下:
     *  每个分区中的第一个元素的唯一ID 为: 该分区索引号
     *  每个分区中的第N个元素的唯一ID 为: 前一个元素的唯一ID 加上 该rdd 的 总的分区数
     *
     * zipIndex : 需要启动spark作业来进行计算
     * zipWithUniqueId : 则不需要
     * */

    rdd2.zip(rdd3).collect().foreach(e=>println(e))

    rdd3.zipWithIndex().collect().foreach(e=>println(e))
    println()
    rdd3.zipWithUniqueId().collect().foreach(e=>println(e))

  }

}
