package com.pcong.base


import org.apache.spark.sql.SparkSession

object SparkBaseSession {

    def newSparkSession():SparkSession={

      SparkSession
        .builder()
        .appName("Spark SQL basic example")
        .master("local")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()
    }

}
