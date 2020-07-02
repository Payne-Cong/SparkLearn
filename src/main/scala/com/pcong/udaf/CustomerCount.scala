package com.pcong.udaf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StringType, StructField, StructType}

object CustomerCount extends UserDefinedAggregateFunction{

  //聚合函数的输入参数数据类型 , Nil 为 一个空的 List , x :: List 为 向头部 追加 x
  override def inputSchema: StructType = StructType(StructField("inputColumn",StringType) :: Nil)

  //中间缓存的数据类型
  override def bufferSchema: StructType = StructType(StructField("sum",LongType) :: Nil)

  //最终输出结果的数据类型
  override def dataType: DataType = LongType

  override def deterministic: Boolean = true

  //初始值，要是DataSet没有数据，就返回该值
  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0) = 0L

  /**
   *
   * @param buffer 相当于把当前分区的,每行数据都需要进行计算,计算的结果保存到buffer中
   * @param input  为 inputSchema 投影的行
   */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(!input.isNullAt(0)){
      buffer(0) = buffer.getLong(0) + 1
    }
  }

  //分区数据汇总
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) +buffer2.getLong(0)
  }

  //计算最终的结果
  override def evaluate(buffer: Row): Long = buffer.getLong(0)
}
