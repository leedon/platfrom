package com.changtu.service

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lubinsu on 5/13/2016
  */
object UserLabel {

  def main(args: Array[String]) {

    val hdfsPath = "hdfs://nameservice1:8020"
    val hdfsURI = new URI(hdfsPath)
    val hdfsConf = new Configuration()
    val hdfs = FileSystem.get(hdfsURI, hdfsConf)
    //匹配文件名
    val srcFiles = hdfsPath.concat("/user/hadoop/tts_bi/tts_bi.bi_user_label.log")
    val targetFile = hdfsPath.concat("/user/hadoop/tts_bi/tts_bi.bi_user_label_string.log")

    val conf = new SparkConf().setAppName("User label's string")
    val sc = new SparkContext(conf)

    val fieldTerminate = "\t"

    //读取用户行为文件和 ip-city 映射数据
    val userLabels = sc.textFile(srcFiles).filter(!_.isEmpty)
    val userLabelsRDD = userLabels.map(_.split(fieldTerminate)).filter(_.length == 5)
      .map(p => p(0))
      .map(_.split("_"))
      .map(p => (p(0), p(1)))
      .reduceByKey(_.concat(",").concat(_))
      .map(p => p._1.concat("\t").concat(p._2))

    //删除目标路径
    val output = new Path(targetFile)
    if (hdfs.exists(output)) hdfs.delete(output, true)

    userLabelsRDD.saveAsTextFile(targetFile)

    sc.stop()
  }
}