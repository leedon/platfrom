package com.changtu

import com.changtu.utils.hdfs.HDFSClient
import com.changtu.utils.spark.SparkClient
import com.twitter.logging.Logger
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by lubinsu on 6/17/2016
  */
class SparkClientSpec extends FlatSpec with Matchers {

  val log = Logger.get()

  "Spark" should "get hdfs uri" in {

    val uri = HDFSClient.hdfs.getUri.toString
    log.info(uri)
    uri should be("hdfs://nameservice1")

  }

  /*it should "return 1" in {
    val sc = SparkClient.getSparkContext("Test")
    SparkClient.getHadoopRDD(sc, "/user/hadoop/tts_ods/tts_ods.bi_ods_task_status.log").count() should be(1)
  }*/
}
