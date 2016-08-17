package com.changtu.util.kafka

/**
  * Created by lubinsu on 6/29/2016.
  */

import java.util.Properties
import java.util.concurrent._
import kafka.consumer.{Consumer, ConsumerConfig, KafkaStream}
import kafka.utils.Logging

class KafkaConsumer(val zookeeper: String,
                    val groupId: String,
                    val topic: String,
                    val delay: Long) extends Logging {

  val config = createConsumerConfig(zookeeper, groupId)
  val consumer = Consumer.create(config)
  var executor: ExecutorService = null

  def shutdown() = {
    if (consumer != null)
      consumer.shutdown()
    if (executor != null)
      executor.shutdown()
  }

  def createConsumerConfig(zookeeper: String, groupId: String): ConsumerConfig = {
    val props = new Properties()
    props.put("zookeeper.connect", zookeeper)
    props.put("group.id", groupId)
    props.put("auto.offset.reset", "largest")
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")
    val config = new ConsumerConfig(props)
    config
  }

  def run(numThreads: Int) = {
    val topicCountMap = Map(topic -> numThreads)
    val consumerMap = consumer.createMessageStreams(topicCountMap)
    val streams = consumerMap.get(topic).get

    executor = Executors.newFixedThreadPool(numThreads)
    var threadNumber = 0
    for (stream <- streams) {
      executor.submit(new ScalaConsumerTest(stream, threadNumber, delay))
      threadNumber += 1
    }
  }
}

object ScalaConsumerExample extends App {
  val example = new KafkaConsumer("bigdata1,bigdata2,bigdata4", "lubinsu_test", "behavior", 10)
  example.run(10)
}

class ScalaConsumerTest(val stream: KafkaStream[Array[Byte], Array[Byte]], val threadNumber: Int, val delay: Long) extends Logging with Runnable {
  def run() {
    val it = stream.iterator()

    while (it.hasNext()) {
      val msg = new String(it.next().message())
      println(msg)
    }

    println("Shutting down Thread: " + threadNumber)
  }
}
