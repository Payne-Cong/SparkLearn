package com.pcong.action

import com.pcong.base.SparkBaseSession
import org.apache.spark.sql.SparkSession

object Test5 {
	def main(args: Array[String]): Unit = {
		val spark = SparkBaseSession.newSparkSession()
		import spark.implicits._
		import org.apache.spark.sql.functions._
		import org.apache.spark.sql.types._

		spark.sparkContext.setLogLevel("WARN")

		val pre_path = "D:\\Dev-Project\\Spark学习指南项目(附带数据)"


		val static = spark.read.json(pre_path+"\\data\\activity-data\\")
		val dataSchema = static.schema

		static.show()

		// 每次最多读取1个文件
		val streaming = spark.readStream.schema(dataSchema).option("maxFilesPerTrigger", 1).json(pre_path+"\\data\\activity-data")

		val activityCounts = streaming.groupBy("gt").count()

		// 转换之后需要调用动作操作启动查询
		val activityQuery = activityCounts.writeStream.queryName("activity_counts")
			.format("memory").outputMode("complete")
			.start()

		// 执行此代码 流操作才会在后台启动
		// activityQuery.awaitTermination()

		for(i <- 1 to 5){
			spark.sql(
				"""
					|select * from activity_counts
					|""".stripMargin)
			Thread.sleep(1000)
		}

		activityQuery.stop()

		val simpleTransform = streaming.withColumn("stairs", expr("gt like '%stairs%'"))
			.where("stairs")
			.where("gt is not null")
			.select("gt", "model", "arrival_time", "creation_time")
			.writeStream
			.queryName("simple_transform")
			.format("memory")
			.outputMode("append")
			.start()
		//rollup(a,b) 分别对 (a,b) (a) () 聚合
		//rollup(a,b,c) 分别对(a,b,c) (a,b) (a) () 聚合
		//cube(a,b) 分别对 (a,b) (a) (b) () 聚合
  	//cube(a,b,c) 分别对(a,b,c) (a,b) (a,c) (b,c) (a) (b) (c) () 聚合

		val deviceModelStats = streaming.cube("gt", "model").avg()
			.drop("avg(Arrival_time)")
			.drop("avg(Creation_Time)")
			.drop("avg(Index)")
			.writeStream.queryName("device_counts").format("memory").outputMode("complete")
			.start()

		deviceModelStats.stop()

		val deviceModelStatsCopy = streaming
			.drop("Arrival_time")
			.drop("Creation_Time")
			.drop("Index")
			.rollup("gt", "model").avg()
			.writeStream.queryName("device_counts_rollup").format("memory").outputMode("complete")
			.start()


		//在Spark 2.2 中，不支持完全外连接、流在右侧的左连接、流在左侧的右连接。结构化流处理也不支持流和流的连接

		val historicalAgg = static.groupBy("gt", "model").avg()
		val deviceModelStats_join = streaming.drop("Arrival_Time", "Creation_Time", "Index")
			.cube("gt", "model").avg()
			.join(historicalAgg, Seq("gt", "model"))
			.writeStream.queryName("device_counts_join").format("memory").outputMode("complete")
			.start()


		// 读取kafka
		// 订阅一个主题
		val ds1 = spark.readStream.format("kafka")
			.option("kafka.bootstrap.servers", "host1:port1,host2:port2")
			.option("subscribe", "topic1")
			.load()
		// 订阅多个主题
		val ds2 = spark.readStream.format("kafka")
			.option("kafka.bootstrap.servers", "host1:port1,host2:port2")
			.option("subscribe", "topic1,topic2")
			.load()
		// 按照某模式订阅
		val ds3 = spark.readStream.format("kafka")
			.option("kafka.bootstrap.servers", "host1:port1,host2:port2")
			.option("subscribePattern", "topic.*")
			.load()

//		数据源中的每一行都具有以下模式：
//		• 键（key）：二进制（binary）。
//		• 值（value）：二进制（binary）。
//		• 主题（topic）：字符串（string）。
//		• 分区（partition）：整型（int）。
//		• 偏移量（offset）：长整型（long）。
//		• 时间戳（timestamp）：长整型（long）。


		//写入kafka接收器
		ds1.selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)")
			.writeStream.format("kafka")
			.option("checkpointLocation", "/to/HDFS-compatible/dir")
			.option("kafka.bootstrap.servers", "host1:port1,host2:port2")
			.start()
		//或者
		ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
			.writeStream.format("kafka")
			.option("kafka.bootstrap.servers", "host1:port1,host2:port2")
			.option("checkpointLocation", "/to/HDFS-compatible/dir")
			.option("topic", "topic1")
			.start()


	}

	def selectTable(spark:SparkSession,table:String):Unit={
		for(i <- 1 to 5){
			spark.sql(
				s"""
					|select * from $table
					|""".stripMargin).show()
			Thread.sleep(1000)
		}
	}

}

