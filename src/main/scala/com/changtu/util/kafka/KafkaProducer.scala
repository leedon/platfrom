package com.changtu.util.kafka

/**
  * Created by lubinsu on 6/29/2016.
  */

import java.util.{Date, Properties}

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.util.Random

class KafkaProducer {

  def sendMsg(): Unit = {


    val events = 100
    val topic = "test"
    val brokers = "bigdata3:9092,bigdata5:9092,bigdata6:9092"
    val rnd = new Random()
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
    val t = System.currentTimeMillis()
    for (nEvents <- Range(0, events)) {
      val runtime = new Date().getTime
      val ip = "192.168.2." + rnd.nextInt(255)
      val msg = runtime + "," + nEvents + ",www.example.com," + ip
      val data = new KeyedMessage[String, String](topic, ip, msg)
      Thread.sleep(1000)
      println(msg)
      producer.send(data)
    }

    println("sent per second: " + events * 1000 / (System.currentTimeMillis() - t))
    producer.close()
  }
}

object KafkaProducer extends App {
  val producer = new KafkaProducer
  producer.sendMsg()
}
