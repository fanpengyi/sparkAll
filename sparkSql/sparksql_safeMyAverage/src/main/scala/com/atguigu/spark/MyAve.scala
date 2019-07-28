package com.atguigu.spark

import org.apache.spark.Aggregator
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.expressions.Aggregator

/**
  * Created by Administrator on 2017/9/19.
  */
class MyAve extends Aggregator[Employee,Average,Double]{

  //用于定义一个聚合函数内部需要的数据结构
  override def zero: Average = ???

  //针对每个分区内部每一个输入来更新你的数据结构
  override def reduce(b: Average, a: Employee): Average = ???

  //用于对于不同分区的结构进行聚合
  override def merge(b1: Average, b2: Average): Average = ???

  //计算输出
  override def finish(reduction: Average): Double = ???

  //用于数据结构他的转换
  override def bufferEncoder: Encoder[Average] = ???

  //用于最终结果的转换
  override def outputEncoder: Encoder[Double] = ???

}
