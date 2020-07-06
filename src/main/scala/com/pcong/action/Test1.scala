package com.pcong.action

import com.pcong.base.SparkBaseSession
import org.apache.spark.rdd.RDD

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
     * zipPartitions :  要求分区数 一致
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

    /**
     *  partitionBy : 根据 partitioner 函数 生成新的 shuffleRdd , 将原先的 rdd 重分区
     *  mapValues : 针对 key / value 的 value 进行map处理
     *  flatMapValues :  针对 key / value 的 value 进行flatMap处理
     *
     */

    val rdd4 = sc.makeRDD(Array((1,"A"),(2,"B"),(3,"C"),(4,"D")),2)
    //查看分区
    val rdd4_check=
    rdd4.mapPartitionsWithIndex((index , iter) => {
        var part_map = scala.collection.mutable.Map[String,List[(Int , String)]]() //可变
        while (iter.hasNext){
          var part_name = "p_" + index;
          var elem = iter.next();
          if(part_map.contains(part_name)){
            var elems = part_map(part_name)
            elems ::= elem;
            part_map(part_name) = elems;
          }else{
            part_map(part_name) = List[(Int,String)](elem)
          }
        }
      part_map.iterator
    })

    rdd4_check.collect().foreach(e=>println(e))

    val rdd4_1 = rdd4.partitionBy(new org.apache.spark.HashPartitioner(2))
    rdd4_1.collect().foreach(e=>println(e))

    rdd4.mapValues(e => e+"_").collect().foreach(e=>println(e))

    val rdd5_1 = sc.makeRDD(Seq(1),1)
    val rdd5 = sc.makeRDD(Seq("a,b,c,d,e"),1)

    rdd5.flatMap(e => {
      e.split(",")
    }).collect().foreach(println)

    rdd5_1.zip(rdd5).flatMapValues(e=>{
      e.split(",").map(x=>x.toUpperCase)
    }).collect().foreach(println)


    val rdd6 = sc.makeRDD(Array(("A",1),("B",2),("A",2),("C",3),("B",2),("C",1)))
    rdd6.groupByKey().collect().foreach(println)

    rdd6.reduceByKey((x,y)=>x+y).collect().foreach(println)

    val rdd_6_1 = sc.makeRDD(Seq(1,2,3,4,5))
    println(rdd_6_1.reduce(_ + _).toString) //  1 + 2 = 3  3+3 = 6 6+4=10 10+5 = 15
    println(rdd_6_1.fold(1)(_ + _))//赋初始值   (1+1) + (2+1) = 5 5+3=8 8+4=12 12+5=17





  }

}
