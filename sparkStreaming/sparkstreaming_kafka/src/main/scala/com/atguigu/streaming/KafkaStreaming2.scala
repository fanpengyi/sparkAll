package com.atguigu.streaming

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaStreaming {

  def main(args: Array[String]): Unit = {

    //conf
    val conf = new SparkConf().setAppName("kafka").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    //创建连接kafka的参数
    val brokeList = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
    val zk = "hadoop102:2181,hadoop103:2181,hadoop104:2181"
    val sourceTopic = "spa"
    val targetTopic = "spa2"
    val consumerGroup = "consumer007"

    //创建kafka的连接对象
    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokeList,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.GROUP_ID_CONFIG -> consumerGroup,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "largest"
    )

    var textKafkaStreaming: InputDStream[(String, String)] = null
    //创建ZK路径
    val topicDirs = new ZKGroupTopicDirs(consumerGroup, targetTopic)
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"

    //从ZK获取offset
    val zkClient = new ZkClient(zk)
    val children = zkClient.countChildren(zkTopicPath)

    //如果有保存，从上一次状态恢复
    if (children > 0) {
      //最终保存的上一次的信息
      var fromOffset: Map[TopicAndPartition, Long] = Map()

      //获取kafka集群元信息
      val topicList = List(targetTopic)
      //获取每个分区leader的消费者
      val getLeaderConsumer = new SimpleConsumer("hadoop102", 9092, 10000, 10000, "getLeader")
      //获取分区的元数据信息
      val request = new TopicMetadataRequest(topicList, 0)
      //返回分区元数据信息
      val response = getLeaderConsumer.send(request)
      //解析分区元数据信息
      val topicMetadataOption = response.topicsMetadata.headOption

      //将（partition->offset）放入Map备用
      val partitions = topicMetadataOption match {
        case Some(tm) => tm.partitionsMetadata.map(pm => (pm.partitionId, pm.leader.get.host)).toMap[Int, String]
        case None => Map[Int, String]()
      }
      //关闭获取leader消费者
      getLeaderConsumer.close()

      //对多个分区进行遍历，每个分区分别获取ZK中的Offset以及leader中的最小Offset
      for (i <- 0 until children) {
        //获取ZK中的offset
        val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/$i")

        //针对分区leader生成消费者
        val consumerMin = new SimpleConsumer(partitions(i), 9092, 10000, 10000, "getMin")
        //获取topic以及partition信息
        val tp = TopicAndPartition(sourceTopic, i)
        //offset元数据信息的请求
        val requestMin = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 1)))
        //leader中的最小offset
        val curOffset = consumerMin.getOffsetsBefore(requestMin).partitionErrorAndOffsets(tp).offsets
        //关闭获取最小offset消费者
        consumerMin.close()

        //校准
        //ZK中的offset
        var nextOffset = partitionOffset.toLong
        //当leader中的最小offset大于ZK中的offset，说明有数据丢失，执行校准逻辑
        if (curOffset.nonEmpty && nextOffset < curOffset.head) {
          nextOffset = curOffset.head
        }

        //将topicPartition以及offset信息放入fromOffset中以供生成sparkStreaming调用
        fromOffset += (tp -> curOffset)
        //关闭ZK客户端
        zkClient.close()

        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
        //根据拿到的offset生成sparkStreaming
        textKafkaStreaming = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffset, messageHandler)
      }
    } else {
      //直接创建
      textKafkaStreaming = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(sourceTopic))
    }

    //获取更新的offset，一定需要在处理之前获取offset信息（思考为什么）
    var offsetRange = Array[OffsetRange]()
    val textKafkaStreaming2 = textKafkaStreaming.transform { rdd =>
      //多个分区，所以多个offset
      offsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    //业务逻辑
    textKafkaStreaming.map(s => "key:" + s._1 + ",value:" + s._2).foreachRDD { rdd =>
      rdd.foreachPartition { iteams =>
        //写回kafka targetTopic
        //创建到kafka的连接
        val pool = KafkaPool(brokeList)
        val kafkaConn = pool.borrowObject()

        //写数据
        for (iteam <- iteams)
          kafkaConn.send(targetTopic, iteam)
        //将连接放入池中
        pool.returnObject(kafkaConn)
      }
      //更新ZK中的数据
      val updataTopicDirs = new ZKGroupTopicDirs(consumerGroup, sourceTopic)
      val updataZKCli = new ZkClient(zk)

      //对每个分区分别更新
      for (offset <- offsetRange) {
        val zkPath = s"${updataTopicDirs.consumerOffsetDir}/${offset.partition}"
        ZkUtils.updatePersistentPath(zkClient, zkPath, offset.fromOffset.toString)
      }
      updataZKCli.close()
    }

    ssc.start()
    ssc.awaitTermination()
  }
}