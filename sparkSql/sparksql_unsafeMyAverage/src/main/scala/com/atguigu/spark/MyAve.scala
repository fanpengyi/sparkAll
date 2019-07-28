package com.atguigu.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StructType}

/**
  * Created by Administrator on 2017/9/19.
  */
class MyAve extends UserDefinedAggregateFunction{

  //聚合函数需要输入参数的数据类型
  override def inputSchema: StructType = ???

  //可以理解为保存聚合函数业务逻辑数据的一个数据结构
  override def bufferSchema: StructType = ???

  // 返回值的数据类型
  override def dataType: DataType = ???

  // 对于相同的输入一直有相同的输出
  override def deterministic: Boolean = true

  //用于初始化你的数据结构
  override def initialize(buffer: MutableAggregationBuffer): Unit = ???

  //用于同分区内Row对聚合函数的更新操作
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = ???

  //用于不同分区对聚合结果的聚合。
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = ???

  //计算最终结果
  override def evaluate(buffer: Row): Any = ???
}
