package com.pcong.action

import com.pcong.base.SparkBaseSession
import com.pcong.udaf.CustomerCount
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import java.util.UUID
import scala.util.Random

object Demo {

  def main(args: Array[String]): Unit = {
    val spark = SparkBaseSession.newSparkSession()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val path = "test.xlsx"

    val testStructType = StructType(
      Array(
        StructField("id", IntegerType, nullable = true),
        StructField("name", StringType, nullable = true)
      )
    )

    val df = spark.read
      .format("com.crealytics.spark.excel")
      .option("useHeader", "false") // 必须，是否使用表头，false的话自己命名表头（_c0）,true则第一行为表头
      .option("treatEmptyValuesAsNulls", "true") // 可选, 是否将空的单元格设置为null ,如果不设置为null 遇见空单元格会报错 默认t: true
      .option("inferSchema", "true") // 可选, default: false
      //.option("dataAddress", "'Sheet2'!A1:G2") // 可选,设置选择数据区域 例如 A1:E2
      //.option("addColorColumns", "true") // 可选, default: false
      //.option("timestampFormat", "yyyy-mm-dd hh:mm:ss") // 可选, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
      //.option("excerptSize", 6) // 可选, default: 10. If set and if schema inferred, number of rows to infer schema from
      //.option("workbookPassword", "pass") // 可选, default None. Requires unlimited strength JCE for older JVMs
      //.option("maxRowsInMemory", 20) // 可选, default None. If set, uses a streaming reader which can help with big files
      //.option("sheetName", "Sheet1")
      .schema(testStructType) // 可选, default: Either inferred schema, or all columns are Strings
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
    ).show()

    reDf.groupBy("name","catname").agg(
      sum("num"), avg("num")
    ).show()

  }

}
