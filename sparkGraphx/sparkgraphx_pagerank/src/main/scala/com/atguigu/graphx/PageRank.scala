package com.atguigu.graphx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wuyufei on 2017/9/22.
  * 
  */
object PageRank extends App{

  //屏蔽日志
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)


  //设定一个SparkConf
  val conf = new SparkConf().setAppName("SimpleGraphX").setMaster("local[4]")
  val sc = new SparkContext(conf)

  val erdd = sc.textFile("C:\\Users\\Administrator\\Desktop\\【尚硅谷】大数据技术之Spark\\3.code\\spark\\sparkGraphx\\doc\\graphx-wiki-edges.txt")
  val edges = erdd.map(x => {val para = x.split("\t");Edge(para(0).trim.toLong,para(1).trim.toLong,0)})

  val vrdd = sc.textFile("C:\\Users\\Administrator\\Desktop\\【尚硅谷】大数据技术之Spark\\3.code\\spark\\sparkGraphx\\doc\\graphx-wiki-vertices.txt")
  val vertices = vrdd.map(x => {val para = x.split("\t");(para(0).trim.toLong,para(1).trim)})

  val graph = Graph(vertices,edges)

  println("**********************************************************")
  println("PageRank计算，获取最有价值的数据")
  println("**********************************************************")

  val prGraph = graph.pageRank(0.001).cache()

  val titleAndPrGraph = graph.outerJoinVertices(prGraph.vertices) {(v, title, rank) => (rank.getOrElse(0.0), title)}

  titleAndPrGraph.vertices.top(10) {
    Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1)
  }.foreach(t => println(t._2._2 + ": " + t._2._1))

  sc.stop()
}
