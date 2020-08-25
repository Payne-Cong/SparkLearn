package com.pcong.action

import java.util.Properties

import com.pcong.base.SparkBaseSession

object Test3 {

  def main(args: Array[String]): Unit = {
    val spark = SparkBaseSession.newSparkSession()
    import spark.implicits._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._

    val pre_path = "D:\\Dev-Project\\Spark学习指南项目(附带数据)\\data\\retail-data\\by-day\\*.csv"

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(pre_path)
      .coalesce(5)


    val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"),
      "MM/d/yyyy H:mm"))
    dfWithDate.createOrReplaceTempView("dfWithDate")

    import org.apache.spark.sql.expressions.Window
    val windowSpec = Window
      .partitionBy("CustomerId", "date")
      .orderBy(col("Quantity").desc)
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)

    val purchaseDenseRank = dense_rank().over(windowSpec)
    val purchaseRank = rank().over(windowSpec)

    dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")
      .select(
        col("CustomerId"),
        col("date"),
        col("Quantity"),
        purchaseRank.alias("quantityRank"),
        purchaseDenseRank.alias("quantityDenseRank"),
        maxPurchaseQuantity.alias("maxPurchaseQuantity"))
//      .show()

    val dfNoNull = dfWithDate.drop()
    dfNoNull.createOrReplaceTempView("dfNoNull")

    spark.sql(
      """
        |SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull
        |GROUP BY customerId, stockCode
        |ORDER BY CustomerId DESC, stockCode DESC
        |""".stripMargin)

    spark.sql(
      """
        |SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull
        |GROUP BY customerId, stockCode GROUPING SETS((customerId, stockCode))
        |ORDER BY CustomerId DESC, stockCode DESC
        |""".stripMargin)


    dfNoNull.rollup("customerId", "stockCode").agg(grouping_id(), sum("Quantity"))
      .orderBy(expr("grouping_id()").desc)


    dfNoNull.cube("customerId", "stockCode").agg(grouping_id(), sum("Quantity"))
      .orderBy(expr("grouping_id()").desc)
      .filter($"grouping_id()" === 1)


    val pivoted = dfWithDate.groupBy("date").pivot("Country").sum() // 透视 , 行转列
    pivoted.where("date > '2011-12-05'").select("date" ,"`USA_sum(Quantity)`")



    val person = Seq(
      (0, "Bill Chambers", 0, Seq(100)),
      (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
      (2, "Michael Armbrust", 1, Seq(250, 100)))
      .toDF("id", "name", "graduate_program", "spark_status")

    val graduateProgram = Seq(
      (0, "Masters", "School of Information", "UC Berkeley"),
      (2, "Masters", "EECS", "UC Berkeley"),
      (1, "Ph.D.", "EECS", "UC Berkeley"))
      .toDF("id", "degree", "department", "school")

    val sparkStatus = Seq(
      (500, "Vice President"),
      (250, "PMC Member"),
      (100, "Contributor"))
      .toDF("id", "status")

    person.createOrReplaceTempView("person")
    graduateProgram.createOrReplaceTempView("graduateProgram")
    sparkStatus.createOrReplaceTempView("sparkStatus")


    val joinExpression = person.col("graduate_program") === graduateProgram.col("id")

    var joinType = Seq("inner","outer","left_outer","right_outer","left_semi","left_anti","cross")

    // left_semi 左半连接 查看左侧的df的值 是否 存在于 右侧的df ,只会返回 左侧的df
    // left_anti 左反连接 与 左半连接相反 它实际上并不包含右侧DataFrame 中的任何值，它只是查看该值是否存在于右侧DataFrame中。
    // 但是，左反连接并不保留第二个 DataFrame 中存在的值，而是只保留在第二个 DataFrame 中没有相应键的值。
    // 可以将反连接视为一个NOT IN SQL类型的过滤器
    person.join(graduateProgram, joinExpression , joinType(0))
    graduateProgram.join(person, joinExpression , joinType(4))
    graduateProgram.join(person, joinExpression , joinType(5))
    person.crossJoin(graduateProgram)

    person.withColumnRenamed("id", "personId")
      .join(sparkStatus, expr("array_contains(spark_status, id)"))

    //read data
    // DataFrameReader.format(...).option("key", "value").schema(...).load()

    //write data
    // DataFrameWriter.format(...).option(...).partitionBy(...).bucketBy(...).sortBy(...).save()


    // 分段读取数据库
//    val props = new java.util.Properties
//    props.setProperty("driver", "")
//    val predicates = Array(
//      "DEST_COUNTRY_NAME = 'Sweden' OR ORIGIN_COUNTRY_NAME = 'Sweden'",
//      "DEST_COUNTRY_NAME = 'Anguilla' OR ORIGIN_COUNTRY_NAME = 'Anguilla'")
//    spark.read.jdbc("url", "tableName", predicates, props)
//    spark.read.jdbc("url", "tableName", predicates, props).rdd.getNumPartitions


    // 滑动窗口读取
//    val colName = "count"
//    val lowerBound = 0L
//    val upperBound = 348113L // 这是数据集最大行数
//    val numPartitions = 10
//    spark.read.jdbc("url","tableName",colName,lowerBound,upperBound,numPartitions,props)



  }

}
