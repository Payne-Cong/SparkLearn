package com.pcong.action

import java.text.SimpleDateFormat
import java.time.{Instant, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.{Date, Locale}

import com.pcong.base.SparkBaseSession
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}

object Test7 {

	// 求 每小时 ip 的 访问量
	def main(args: Array[String]): Unit = {
		val spark = SparkBaseSession.newSparkSession()
		import spark.implicits._
		import org.apache.spark.sql.functions._

		val logSchema = new StructType(Array(
			new StructField("remote_addr", StringType, true),
			new StructField("host", StringType, true),
			new StructField("time", StringType, false),
			new StructField("request", StringType, false),
			new StructField("status", StringType, false),
			new StructField("size", StringType, false),
			new StructField("referer", StringType, false),
			new StructField("user_agent", StringType, false),
			new StructField("forwarded", StringType, false),
			new StructField("request_time", StringType, false),
			new StructField("upstream_response_time", StringType, false),
			new StructField("upstream_addr", StringType, false),
			new StructField("url", StringType, false),
			new StructField("request_url", StringType, false)
		))

		val fileName = "example.log"
		val source = spark.read.schema(logSchema).json(fileName)

		val df = source.withColumn("time_1", to_timestamp($"time", "d/MMM/yyyy:h:m:s"))
			.withColumn("time_2", date_trunc("HOUR", $"time_1"))

		// 求 一天每小时 ip 的 每个接口 访问量
		df.groupBy("remote_addr", "time_2","url").agg(count("remote_addr").as("pv")).show()

		// 求 一天每小时  每个接口 访问量
		df.groupBy("url","time_2").agg(countDistinct("remote_addr").as("pu")).show()

	}

}


case class Log(
	              remote_addr:String,
	              host:String,
	              time:String,
	              request:String,
	              status:String,
	              size:String,
	              referer:String,
	              user_agent:String,
	              forwarded:String,
	              request_time:Double,
	              upstream_response_time:String,
	              upstream_addr:String,
	              url:String,
	              request_url:String
              )
