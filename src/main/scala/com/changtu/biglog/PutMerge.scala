package com.changtu.biglog

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
  * Created by lubinsu on 2016/4/26
  */
object PutMerge {

  //ip映射表
  case class BiOdsCmsIps(starIp: String, endIp: String, cityId: Long)

  //用户行为表
  case class UserBhvr(unique_id: String, user_id: String, type_cd: String, log_url: String, log_his_refer: String, title: String, char_set: String, log_platform: String, cookie_enable: String, os_language: String, sys_language: String, sr: String, value: String, client_ip: String, server_record_time: String, user_agent: String, browser: String, fk_user_id: String, page_id: String, visit_source: String, visit_path: String, client_ip_format: String)

  /**
    * 转换ip为一串数值，如 118.75.198.121 转为：118075198121
    * @param ip 传入client_ip字段值
    * @return
    */
  def fixLength(ip: String): String = {
    if (ip.trim.length == 0) {
      "0"
    } else {
      val ip0 = ip.split(",")(0)
      if (ip0.split("\\.").length == 4) {
        val ip1 = ip0.split("\\.")(0)
        val ip2 = ip0.split("\\.")(1)
        val ip3 = ip0.split("\\.")(2)
        val ip4 = ip0.split("\\.")(3)

        "000".substring(0, 3 - ip1.length) + ip1 +
          "000".substring(0, 3 - ip2.length) + ip2 +
          "000".substring(0, 3 - ip3.length) + ip3 +
          "000".substring(0, 3 - ip4.length) + ip4
      } else {
        "0"
      }
    }
  }

  def getCity(ipFormat: String, ipList: Array[Row]) : String = {
    val cityRow = ipList.find({x:Row => x.getString(0).toLong < ipFormat.toLong & x.getString(1).toLong > ipFormat.toLong})
    cityRow match {
      case Some(_) => cityRow.get.getLong(2).toString
      case None => "0"
    }
  }

  def main(args: Array[String]) {
    //namenode uri
    val hdfsPath = "hdfs://nameservice1:8020"
    val hdfsURI = new URI(hdfsPath)
    val hdfsConf = new Configuration()
    val hdfs = FileSystem.get(hdfsURI, hdfsConf)
    val conf = new SparkConf().setAppName("put merge")
    val sc = new SparkContext(conf)
    val srcDir = args(0)
    val targetDir = args(2)
    val hourDuration = args(3).toInt
    val filesNameFormat = args(1).replace("{DATE_TIME}", DateTime.now().plusHours(hourDuration).toString("yyyyMMddHH"))

    val srcFiles = hdfsPath.concat(srcDir).concat(filesNameFormat)
    val targetFile = hdfsPath.concat(targetDir).concat(DateTime.now().plusHours(hourDuration).toString("yyyyMMdd")).concat("/").concat(DateTime.now().plusHours(hourDuration).toString("HH"))
    val srcFileRDD = sc.textFile(srcFiles)
    val output = new Path(targetFile)

    if (hdfs.exists(output)) hdfs.delete(output, true)

    println("merging files ")
    srcFileRDD.repartition(1).saveAsTextFile(targetFile)

    //delete old files
    val regex = "(".concat(filesNameFormat).concat(")").r
    val hdfsStatus = hdfs.listStatus(new Path(hdfsPath.concat(srcDir)))
    hdfsStatus.foreach(x => {
      x.getPath.getName match {
        case regex(url) =>
          println("deleting file : " + url)
          hdfs.delete(x.getPath, true)
        case _ => ()
      }
    })

    sc.stop()
  }
}