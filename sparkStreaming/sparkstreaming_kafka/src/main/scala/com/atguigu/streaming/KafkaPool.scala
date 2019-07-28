package com.atguigu.streaming

import java.util.Properties

import org.apache.commons.pool2.BasePooledObjectFactory
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

//封装一个kafka连接对象
class KafkaProxy(brokers: String) {

  private val pros: Properties = new Properties()
  pros.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  pros.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  pros.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  private val kafkaProducer = new KafkaProducer[String, String](pros)

  def send(topic: String, key: String, value: String): Unit = {
    kafkaProducer.send(new ProducerRecord[String, String](topic, key, value))
  }

  def send(topic: String, value: String): Unit = {
    kafkaProducer.send(new ProducerRecord[String, String](topic, value))
  }

  def close(): Unit = {
    kafkaProducer.close()
  }
}

//创建一个用于创建对象的工厂
class KafkaProxyFactory(brokers: String) extends BasePooledObjectFactory[KafkaProxy] {

  override def create() = new KafkaProxy(brokers)

  override def wrap(t: KafkaProxy) = new DefaultPooledObject[KafkaProxy](t)
}

object KafkaPool {
  //声明一个连接池对象
  private var kafkaProxyPool: GenericObjectPool[KafkaProxy] = _

  def apply(brokers: String): GenericObjectPool[KafkaProxy] = {
    if (kafkaProxyPool == null) {
      KafkaPool.synchronized {
        if (kafkaProxyPool == null)
          kafkaProxyPool = new GenericObjectPool[KafkaProxy](new KafkaProxyFactory(brokers))
      }
    }
    kafkaProxyPool
  }
}