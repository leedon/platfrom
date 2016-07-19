package com.changtu.label

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lubinsu on 5/13/2016
  */
object UserLabelCode {

  def main(args: Array[String]) {

    val hdfsPath = "hdfs://nameservice1:8020"
    val hdfsURI = new URI(hdfsPath)
    val hdfsConf = new Configuration()
    val hdfs = FileSystem.get(hdfsURI, hdfsConf)
    //匹配文件名
    val srcFiles = hdfsPath.concat("/user/hadoop/tts_bi/tts_bi.bi_user_label.log")
    val targetFile = hdfsPath.concat("/user/hadoop/tts_bi/tts_bi.bi_user_label_code_string.log")

    val conf = new SparkConf().setAppName("User label's string")
    val sc = new SparkContext(conf)

    val fieldTerminate = "\t"

    //读取用户行为文件和 ip-city 映射数据
    val userLabels = sc.textFile(srcFiles).filter(!_.isEmpty)
    val userLabelsRDD = userLabels.map(_.split(fieldTerminate)).filter(_.length == 5)
      /*
      数据文件结构
      SELECT a.user_id || '_' || a.label_id user_label_id,
      a.label_code label_code,
      a.status status,
      to_char(a.create_dt, 'yyyy-mm-dd hh24:mi:ss') create_dt,
      to_char(a.modify_dt, 'yyyy-mm-dd hh24:mi:ss') modify_dt
      FROM   tts_bi.bi_user_label a
      */

      //获取 userId_labelId,labelCode
      //.map(p => p(0).split("_")(0).concat("^").concat(p(0).split("_")(1)).concat("^").concat(p(1)))
      //.map(_.split("^"))
      //配置为 (userId_labelCode, labelId) 的结构
      .map(p => (p(0).split("_")(0).concat("_").concat(p(1)), p(0).split("_")(1)))
      .reduceByKey(_.concat(",").concat(_))
      .map(p => p._1.concat("\t").concat(p._2))

    //删除目标路径
    val output = new Path(targetFile)
    if (hdfs.exists(output)) hdfs.delete(output, true)

    userLabelsRDD.saveAsTextFile(targetFile)

    sc.stop()
  }
}