package com.changtu.service

import com.changtu.util.hdfs.HDFSUtils
import com.changtu.util.redis.RedisUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lubinsu on 9/9/2016.

  * 针对hashmap格式的存储方式
  *
  */
object LoadToRedis extends App {

  //hdfs://nameservice1/user/hadoop/tts_ods/tts_ods.v_exp_balance.log
  if (args.length < 4) {
    System.err.println("Usage: com.changtu.service.LoadToRedis <srcFile> <fieldPosition> <valuePosition> <Key>")
    System.exit(1)
  }

  //namenode uri
  val fieldTerminate = "\001"
  val hdfs = HDFSUtils.getHdfs
  val hdfsPath = hdfs.getUri.toString
  val conf = new SparkConf().setAppName("com.changtu.service.LoadToRedis")
  val sc = new SparkContext(conf)

  val Array(srcFile, fieldPosition, valuePosition, key) = args

  val srcFiles = hdfsPath.concat(srcFile)

  val srcFileRDD = sc.textFile(srcFiles).map(_.split(fieldTerminate)).map(p => (p(fieldPosition.toInt), p(valuePosition.toInt)))

  srcFileRDD.foreachPartition(part => {
    val data = part.toMap
    RedisUtils.hmset(key, data)
  })
}
