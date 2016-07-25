package com.changtu.examples

import com.changtu.util.Logging
import com.changtu.util.hdfs.HDFSUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lubinsu on 7/20/2016.
  */
object AccessLog extends Logging {

  def main(args: Array[String]) {

    //115.28.149.83
    val conf = new SparkConf().setAppName("Access Log Analyze")
    val sc = new SparkContext(conf)
    //创建大日志模型代码
    case class AccessLog(typeCd: String, ipAddr: String, recordTime: String, url: String, status: String)

    val fieldTerminate = "€"

    val nginxLog = sc.textFile("hdfs://nameservice1:/user/hadoop/accesslog/access_file_20160720.log").filter(!_.isEmpty).map(_.split(fieldTerminate, 5)).filter(_.length == 5)
    val regex = ".*\\?".r
    //val regex = "(startCityUrl=\\w*&)".r
    val accessCnt = nginxLog.map(p => AccessLog(p(0), p(1), p(2), (regex findFirstIn p(3)).getOrElse(p(3)), p(4)))
      //.filter(p => p.ipAddr == "115.28.149.83")
      .map(p => (p.url, 1)).reduceByKey(_ + _)
    /*val accessCnt = nginxLog.filter(p => p(3).contains("/ticket/querySchListOnPage.htm")).map(p => AccessLog(p(0), p(1), p(2), (regex findFirstIn p(3)).getOrElse("").replace("startCityUrl=", "").replace("&", ""), p(4)))
      .filter(p => p.ipAddr == "115.28.149.83")
      .map(p => (p.url, 1)).reduceByKey(_ + _)*/

    HDFSUtils.delete("/user/hadoop/accesslog/20160720/")
    accessCnt.repartition(1).saveAsTextFile("hdfs://nameservice1:/user/hadoop/accesslog/20160720/")
  }
}
