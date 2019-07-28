package com.atguigu.graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by wuyufei on 10/09/2017.
  */
object HelloWorld {

  val logger = LoggerFactory.getLogger(HelloWorld.getClass)

  def main(args: Array[String]) {


    //需要创建一个SparkConf来包含整个的Spark配置
    val conf = new SparkConf().setMaster("local[3]").setAppName("WC")
    //创建一个SParkContext，对于图计算来说没有比较特殊的入口对象。
    val sc = new SparkContext(conf)

    //创建了一个顶点的集合，VertexID是一个Long类型，我的顶点属性是一个二元组。
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))

    // 创建一个边的集合，我的边属性是String类型
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

    val defaultUser = ("John Doe", "Missing")

    //创建一张图，传入了我的顶点和边。
    val graph = Graph(users, relationships, defaultUser)

    // 过滤图上的所有顶点，如果顶点属性的第二个值是postdoc 那就过滤出来。计算总的满足条件的顶点个数
    val verticesCount = graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count
    println(verticesCount)

    // 计算满足条件的边的个数，条件是边的源顶点ID 大于 目标顶点的ID
    val edgeCount = graph.edges.filter(e => e.srcId > e.dstId).count
    println(edgeCount)

    //关闭SparkContext
    sc.stop()
  }

}




