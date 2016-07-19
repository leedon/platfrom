package com.changtu.streaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark streaming handler
  * Created by lubinsu on 2016/3/7
  */
object StreamingHandler {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FlumeNG sink").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))

    val stream = FlumeUtils.createStream(ssc, "172.19.0.5", 22224, StorageLevel.MEMORY_AND_DISK)
    println("started ... ")
    //stream.map(e => "FlumeNG:header:" + e.event.get(0).toString + "body: " + new String(e.event.getBody.array)).print
    val odd = stream.map(line => new String(line.event.getBody.array).split(",")) /*.filter(_(0).toInt % 2 == 1)*/ .map(x => x.mkString("|"))
    odd.foreachRDD((x, y) => x.saveAsTextFile("hdfs://172.19.0.95:9999/user/root/input/flume/flume-" + y.milliseconds))
    //odd.saveAsTextFiles("hdfs://172.19.0.95:9999/user/root/input/flume/flume")
    ssc.start()
    ssc.awaitTermination()
    sc.stop()
    println("ended ... ")
  }
}
