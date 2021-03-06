package com.changtu.util.kafka

/**
  * Created by lubinsu on 6/29/2016.
  */

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

class KafkaProducer {

  def sendMsg(topic: String, brokers: String, key: String, msg: String): Unit = {

    //    val topic = "test"
    //    val brokers = "bigdata3:9092,bigdata5:9092,bigdata6:9092"
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("bootstrap.servers", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("producer.type", "async")

    props.put("acks", "all")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)
    val data = new KeyedMessage[String, String](topic, key, msg)
    producer.send(data)

    producer.close()

  }
}

object KafkaProducer extends App {
  KafkaUtils.send("test", "hello_key", "hello_msg")
}
