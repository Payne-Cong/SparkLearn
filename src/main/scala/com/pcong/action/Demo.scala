package com.pcong.action

import java.util.UUID

import com.pcong.base.SparkBaseSession
import com.pcong.udaf.CustomerCount

import scala.util.Random

object Demo {

  def main(args: Array[String]): Unit = {
    val spark = SparkBaseSession.newSparkSession()
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val path = "test.xlsx"
    val df =  spark.read.format("com.crealytics.spark.excel").option("useHeader", "true")
      //这三行可以要，可以不要
      //.option("timestampFormat", "MM-dd-yyyy HH:mm:ss")
      //.option("inferSchema", "false")
      //.option("workbookPassword", "None")
    .load(path)

    df.filter($"id" < 10)
    df.groupBy("id","name").agg(
      count("id").as("count")
    )

    val generateUUID = udf(() => UUID.randomUUID().toString)

    val randomInt = udf(() => Random.nextInt(50))

    val strUpper = udf((str: String) => str.toUpperCase())

    val strCat = udf((str1: String,str2: String) => str2+str1)

    val reDf = df.withColumn("uuid", generateUUID())
      .withColumn("num",randomInt())
      .withColumn("catname" , strCat($"id",$"name"))

    reDf.groupBy("name","catname").agg(
      CustomerCount(lit("catname")).as("count")
    ).show()

    reDf.groupBy("name","catname").agg(
      ("num" -> "sum"), ("num" -> "avg")
    )

    reDf.groupBy("name","catname").agg(
      sum("num"), avg("num")
    )

  }

}
