package com.changtu.service

import com.changtu.util.hdfs.HDFSUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lubinsu on 5/13/2016
  */
object UserLabelCode {

  def main(args: Array[String]) {

    val hdfs = HDFSUtils.getHdfs
    val hdfsPath = hdfs.getUri.toString
    //匹配文件名
    val srcFiles = hdfsPath.concat("/user/hadoop/tts_bi/tts_bi.bi_user_label.log")
    val targetFile = hdfsPath.concat("/user/hadoop/tts_bi/tts_bi.bi_user_label_code_string.log")

    val conf = new SparkConf().setAppName("User label's string")
    val sc = new SparkContext(conf)

    val fieldTerminate = "\t"

    //读取用户行为文件和 ip-city 映射数据
    val userLabels = sc.textFile(srcFiles).filter(!_.isEmpty)
    val userLabelsRDD = userLabels.map(_.split(fieldTerminate)).filter(_.length == 5).filter(p => p(2) == "1")
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

    //删除重建目标文件
    HDFSUtils.delete(targetFile)

    userLabelsRDD.saveAsTextFile(targetFile)

    sc.stop()
  }
}
