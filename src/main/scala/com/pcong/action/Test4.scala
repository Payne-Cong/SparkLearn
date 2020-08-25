package com.pcong.action

import com.pcong.base.SparkBaseSession
import org.apache.spark.util.LongAccumulator

object Test4 {
	def main(args: Array[String]): Unit = {
		val spark = SparkBaseSession.newSparkSession()
		import spark.implicits._
		import org.apache.spark.sql.functions._
		import org.apache.spark.sql.types._

		val myCollection = "Spark The Definitive Guide Is Big Data Processing Made Simple"
			.split(" ")
		val words = spark.sparkContext.parallelize(myCollection, 2)

		words.map(objectRow => (objectRow.toLowerCase,1)).reduceByKey(_+_)
		words.map((_,1)).reduceByKey(_+_)
		val keywords = words.keyBy(_.toUpperCase.toSeq(0).toString)// 创建key,value 为 origin value
		keywords.mapValues(_.toLowerCase) // 对values 进行map
		keywords.lookup("T")

		val distinctChars = words.flatMap(word => word.toLowerCase.toSeq).distinct
			.collect()
		import scala.util.Random
		val sampleMap = distinctChars.map(c => (c, new Random().nextDouble())).toMap
		words.map(word => (word.toLowerCase.toSeq(0), word))
			.sampleByKey(true, sampleMap, 6L) // 通过一组key,对rdd进行采样,采样数量=math.ceil(numItems * samplingRate)


		val chars = words.flatMap(word => word.toLowerCase.toSeq)
		val KVcharacters = chars.map(letter => (letter, 1))
		def maxFunc(left:Int, right:Int) = math.max(left, right)
		def addFunc(left:Int, right:Int) = left + right
		val nums = spark.sparkContext.parallelize(1 to 30, 5)

		KVcharacters.groupByKey().map(row => (row._1, row._2.reduce(addFunc))).collect() // 先归并,再计算, 如果 key 对应的values 过多 ,容易影响性能
		KVcharacters.reduceByKey(addFunc).collect() // 逐次计算 ,最后整合,计算过程发生在分组期间,不会产生shuffle

		//aggregate，此函数需要一个 null 值和一个起始值，并需要你指定两个
		//不同的函数，第一个函数执行单个分区内聚合，第二个执行多个分区间聚合。起始值在两个聚合级别都使用
		nums.aggregate(0)(maxFunc, addFunc)//在驱动器上执行最终聚合
		val depth = 3
		nums.treeAggregate(0)(maxFunc,addFunc,depth)//创建从执行器到执行器传输聚合结果的树

		val kvFunc = (key:Any)=>(key,1)

		//(1)注册累计器
		val accChina = new LongAccumulator
		spark.sparkContext.register(accChina,"China")
		//(2)
		//val accChina2 = spark.sparkContext.longAccumulator("China")


		val flights = spark.read
			.parquet("D:\\Dev-Project\\Spark学习指南项目(附带数据)\\data\\flight-data\\parquet\\2010-summary.parquet")
			.as[Flight]


		def accChinaFunc(flight_row: Flight) = {
			val destination = flight_row.DEST_COUNTRY_NAME
			val origin = flight_row.ORIGIN_COUNTRY_NAME
			if (destination == "China") {
				accChina.add(flight_row.count.toLong)
			}
			if (origin == "China") {
				accChina.add(flight_row.count.toLong)
			}
		}

		flights.foreach(row => accChinaFunc(row))
		flights.filter($"DEST_COUNTRY_NAME" === lit("China") or $"ORIGIN_COUNTRY_NAME" === lit("China"))
  			.agg(sum("count"))
		//
		accChina.value

		//自定义累加器
		import scala.collection.mutable.ArrayBuffer
		import org.apache.spark.util.AccumulatorV2
		val arr = ArrayBuffer[BigInt]()
		class EvenAccumulator extends AccumulatorV2[BigInt, BigInt] {
			private var num:BigInt = 0
			def reset(): Unit = {
				this.num = 0
			}
			def add(intValue: BigInt): Unit = {
				if (intValue % 2 == 0) {  //对偶数进行累加
					this.num += intValue
				}
			}
			def merge(other: AccumulatorV2[BigInt,BigInt]): Unit = {
				this.num += other.value
			}
			def value():BigInt = {
				this.num
			}
			def copy(): AccumulatorV2[BigInt,BigInt] = {
				new EvenAccumulator
			}
			def isZero():Boolean = {
				this.num == 0
			}
		}
		val acc = new EvenAccumulator
		val newAcc = spark.sparkContext.register(acc, "evenAcc")
		// in Scala
		acc.value // 0
		flights.foreach(flight_row => acc.add(flight_row.count))
		acc.value // 31390













	}

	case class Flight(DEST_COUNTRY_NAME: String,
	                  ORIGIN_COUNTRY_NAME: String, count: BigInt)

}
