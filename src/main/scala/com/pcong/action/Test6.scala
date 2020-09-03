package com.pcong.action

import com.pcong.base.SparkBaseSession

object Test6{

	def main(args: Array[String]): Unit = {
		val spark = SparkBaseSession.newSparkSession()
		import spark.implicits._
		import org.apache.spark.sql.functions._
		import org.apache.spark.sql.types._

		spark.sparkContext.setLogLevel("WARN")

		val pre_path = "D:\\Dev-Project\\Spark学习指南项目(附带数据)"
		spark.conf.set("spark.sql.shuffle.partitions", 5)
		val static = spark.read.json(s"$pre_path\\data\\activity-data")
		val streaming = spark
			.readStream
			.schema(static.schema)
			.option("maxFilesPerTrigger", 10)
			.json(s"$pre_path\\data\\activity-data")

		val withEventTime = streaming.selectExpr(
			"*",
			"cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time")

//  在给定的窗口计数,在10分钟内的滚动窗口进行计数,这些时间窗口不会发生重叠
//		import org.apache.spark.sql.functions.{window, col}
//		withEventTime.groupBy(window(col("event_time"), "10 minutes")).count()
//			.writeStream
//			.queryName("events_per_window")
//			.format("memory")
//			.outputMode("complete")
//			.start()

//		import org.apache.spark.sql.functions.{window, col}
//		withEventTime.groupBy(window(col("event_time"), "10 minutes"), $"User").count()
//			.writeStream
//			.queryName("events_per_window")
//			.format("memory")
//			.outputMode("complete")
//			.start()

		// 滚动窗口,时间窗口会发生重叠,这里每隔5分钟就启动一个时间窗口
//		import org.apache.spark.sql.functions.{window, col}
//		withEventTime.groupBy(window(col("event_time"), "10 minutes", "5 minutes"))
//			.count()
//			.writeStream
//			.queryName("events_per_window")
//			.format("memory")
//			.outputMode("complete")
//			.start()


		//水位是给定事件或事件集之后的一个时间长度，在该时间长度之后我们不希望再看到来自该时间长度之前的任何数据。
		import org.apache.spark.sql.functions.{window, col}
		withEventTime
			.withWatermark("event_time", "5 hours")//设置水位
			.groupBy(window(col("event_time"), "10 minutes", "5 minutes"))
			.count()
			.writeStream
			.queryName("events_per_window")
			.format("memory")
			.outputMode("complete")
			.start()

		//流中去除重复项
		import org.apache.spark.sql.functions.expr
		withEventTime
			.withWatermark("event_time", "5 seconds")
			.dropDuplicates("User", "event_time")
			.groupBy("User")
			.count()
			.writeStream
			.queryName("deduplicated")
			.format("memory")
			.outputMode("complete")
			.start()


		/**
		 *
		 * 超时时间是指在标记某个中间状态为超时（time-out）之前应该等待多长时间，
		 * 超时时间是作用在每组上的一个全局参数。超时可以基于处理时间 (GroupStateTimeout.ProcessingTimeTimeout)，
		 * 也可以基于事件时间(GroupStateTimeout. EventTimeTimeout)。
		 * 使用超时，请先检查超时时间的设置，
		 * 你可以通过检查state.hasTimedOut标志或检查值迭代器是否为空来获取此信息。
		 * 你需要设置一些状态（必须定义状态，而不是删除状态）才能设置超时。
		 *
		 * 基于处理时间的超时，可以通过调用GroupState.setTimeoutDuration设置超时时间
		 * 由于处理时间超时是基于时钟时间的，所以系统时钟的不同会对超时有影响，这意味着时区不同和时钟偏差是需要给予考虑的。
		 */

		/**
		 * mapGroupsWithState
		 * 三个类定义： 输入定义、状态定义、以及可选的输出定义。
		 * 基于键、事件迭代器和先前状态的一个更新状态的函数。
		 * 超时时间参数 (如“超时”部分所述)。
		 */

		//假如我们正在处理传感器数据，我们要查找给定用户在数据集中执行某项活动的第一个和最后一个时间戳
		//这意味着分组操作的键是用户和活动组合



		// 状态更新函数,定义如何根据给定行更新状态
		def updateUserStateWithEvent(state:UserState, input:InputRow):UserState = {
			if (Option(input.timestamp).isEmpty) {
				return state
			}
			if (state.activity == input.activity) {
				if (input.timestamp.after(state.end)) {
					state.end = input.timestamp
				}
				if (input.timestamp.before(state.start)) {
					state.start = input.timestamp
				}
			} else {
				if (input.timestamp.after(state.end)) {
					state.start = input.timestamp
					state.end = input.timestamp
					state.activity = input.activity
				}
			}
			state
		}

		// 通过函数来定义根据每一批次行来更新状态
		import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode, GroupState}
		def updateAcrossEvents(user:String,
		                       inputs: Iterator[InputRow],
		                       oldState: GroupState[UserState]):UserState = {
			var state:UserState = if (oldState.exists) oldState.get else UserState(user,
				"",
				new java.sql.Timestamp(6284160000000L),
				new java.sql.Timestamp(6284160L)
			)
			// 我们直接指定了可以用于比较的旧日期，
			// 并且根据数据值进行及时更新
			for (input <- inputs) {
				state = updateUserStateWithEvent(state, input)
				oldState.update(state)
			}
			state
		}

		import org.apache.spark.sql.streaming.GroupStateTimeout
		withEventTime
			.selectExpr("User as user",
				"cast(Creation_Time/1000000000 as timestamp) as timestamp", "gt as activity")
			.as[InputRow]
			.groupByKey(_.user)
			.mapGroupsWithState(GroupStateTimeout.NoTimeout)(updateAcrossEvents)
			.writeStream
			.queryName("events_per_window_g")
			.format("memory")
			.outputMode("update")
			.start()


		Test5.selectTable(spark,"events_per_window_g")

	}

	/**
	 * 输入定义、状态定义
	 */
	case class InputRow(user:String, timestamp:java.sql.Timestamp, activity:String)
	case class UserState(user:String,
	                     var activity:String,
	                     var start:java.sql.Timestamp,
	                     var end:java.sql.Timestamp)
}
