package com.changtu.label

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by lubinsu on 2016/6/8.
  */
object LabelUser {

  val hdfsPath = "hdfs://nameservice1:8020"

  val hdfsURI = new URI(hdfsPath)

  val hdfsConf = new Configuration()

  val hdfs = FileSystem.get(hdfsURI, hdfsConf)


  def main(args: Array[String]) {


    //匹配文件名
    val srcFiles = hdfsPath.concat("/user/hadoop/tts_bi/tts_bi.bi_user_label.log")
    val targetFile = hdfsPath.concat("/user/hadoop/tts_bi/tts_bi.bi_label_user_string.log")

    val conf = new SparkConf().setAppName("Label's user string")
    val sc = new SparkContext(conf)

    val fieldTerminate = "\t"

    //读取用户行为文件和 ip-city 映射数据
    val labelUsers = sc.textFile(srcFiles).filter(!_.isEmpty)
    val labelUsersRDD = labelUsers.map(_.split(fieldTerminate)).filter(_.length == 5)
      .map(p => p(0))
      .map(_.split("_"))
      .map(p => (p(1), p(0)))
      .reduceByKey(_.concat(",").concat(_))
      .map(p => p._1.concat("\t").concat(p._2))

    //删除目标路径
    val output = new Path(targetFile)
    if (hdfs.exists(output)) hdfs.delete(output, true)

    labelUsersRDD.saveAsTextFile(targetFile)

    sc.stop()
  }
}
