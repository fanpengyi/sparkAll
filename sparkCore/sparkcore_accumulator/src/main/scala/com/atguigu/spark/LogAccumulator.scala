package com.atguigu.spark

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._


/**
  * Created by wuyufei on 05/09/2017.
  */
class LogAccumulator extends AccumulatorV2[String, java.util.Set[String]] {

  // 定义一个累加器的内存结构，用于保存带有字母的字符串。
  private val _logArray: java.util.Set[String] = new java.util.HashSet[String]()

  // 重写方法检测累加器内部数据结构是否为空。
  override def isZero: Boolean = {
    //检查_logArray 是否为空
    _logArray.isEmpty
  }

  // 重置你的累加器数据结构
  override def reset(): Unit = {
    //clear方法清空_logArray的所有内容
    _logArray.clear()
  }

  // 提供转换或者行动操作中添加累加器值的方法
  override def add(v: String): Unit = {
    // 将带有字母的字符串添加到_logArray内存结构中
    _logArray.add(v)
  }

  // 提供将多个分区的累加器的值进行合并的操作函数
  override def merge(other: AccumulatorV2[String, java.util.Set[String]]): Unit = {
    // 通过类型检测将o这个累加器的值加入到当前_logArray结构中
    other match {
      case o: LogAccumulator => _logArray.addAll(o.value)
    }

  }

  // 输出我的value值
  override def value: java.util.Set[String] = {
    java.util.Collections.unmodifiableSet(_logArray)
  }

  override def copy(): AccumulatorV2[String, java.util.Set[String]] = {
    val newAcc = new LogAccumulator()
    _logArray.synchronized{
      newAcc._logArray.addAll(_logArray)
    }
    newAcc
  }
}

// 过滤掉带字母的
object LogAccumulator {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("LogAccumulator").setMaster("local[*]")
    val sc=new SparkContext(conf)

    val accum = new LogAccumulator
    sc.register(accum, "logAccum")
    val sum = sc.parallelize(Array("1", "2a", "3", "4b", "5", "6", "7cd", "8", "9"), 2).filter(line => {
      val pattern = """^-?(\d+)"""
      val flag = line.matches(pattern)
      if (!flag) {
        accum.add(line)
      }
      flag
    }).map(_.toInt).reduce(_ + _)

    println("sum: " + sum)
    for (v <- accum.value) print(v + " ")
    sc.stop()
  }
}
