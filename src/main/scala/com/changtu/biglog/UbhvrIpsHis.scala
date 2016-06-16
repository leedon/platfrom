package com.changtu.biglog

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by lubinsu on 5/13/2016
  */
object UbhvrIpsHis {

  case class BiOdsCmsIps(starIp: String, endIp: String, cityId: Long)

  def fixLength(ip: String): String = {
    if (ip.trim.length == 0) {
      "0"
    } else {
      val ip0 = ip.split(",")(0)
      if (ip0.split("\\.").length == 4) {
        val ip1 = ip0.split("\\.")(0).trim
        val ip2 = ip0.split("\\.")(1).trim
        val ip3 = ip0.split("\\.")(2).trim
        val ip4 = ip0.split("\\.")(3).trim

        try {
          "000".substring(0, 3 - ip1.length) + ip1 +
            "000".substring(0, 3 - ip2.length) + ip2 +
            "000".substring(0, 3 - ip3.length) + ip3 +
            "000".substring(0, 3 - ip4.length) + ip4
        } catch {
          case e: StringIndexOutOfBoundsException => "0"
        }
      } else {
        "0"
      }
    }
  }

  def getCity(ipFormat: String, ipMap: scala.collection.Map[String, List[BiOdsCmsIps]]): String = {
    try {
      val cityRow = ipMap.get(ipFormat.substring(0, 3))

      cityRow match {
        case Some(_) =>
          val result = cityRow.get.filter(p => p.starIp.toLong <= ipFormat.toLong & p.endIp.toLong >= ipFormat.toLong)
          if (result.nonEmpty) {
            result.head.cityId.toString
          } else {
            "0"
          }
        case None => "0"
      }
    } catch {
      case e: StringIndexOutOfBoundsException => "0"
    } finally {}
  }

  //获取JDBC配置信息
  def getJdbcProps: Map[String, String] = {
    var props: Map[String, String] = Map()
    var filePath = System.getenv("CONF_HOME")
    if (filePath == null || filePath.length == 0) {
      filePath = "/appl/conf"
    }

    Source.fromFile(filePath + "/jdbc.properties").getLines().filterNot(_.startsWith("#")).map(_.split("=")).foreach(p => {
      props += (p(0) -> p(1))
    })
    props
  }


  def main(args: Array[String]) {

    // 需要处理的文件名，可以通过模糊匹配来做
    val userBhvrDir = args(0)

    // 是否在HDFS上要额外保存一份
    val saveF = args(1)
    val hdfsPath = "hdfs://nameservice1:8020"
    val hdfsURI = new URI(hdfsPath)
    val hdfsConf = new Configuration()
    val hdfs = FileSystem.get(hdfsURI, hdfsConf)
    //匹配文件名
    val srcFiles = hdfsPath.concat(userBhvrDir)

    val conf = new SparkConf().setAppName("UbhvrIpsHis")
    val sc = new SparkContext(conf)

    val fieldTerminate = "\001"
    //val props = getJdbcProps

    //读取用户行为文件和 ip-city 映射数据
    val bhvrHourly = sc.textFile(srcFiles).filter(!_.isEmpty)
    val ipRdd = sc.textFile(hdfsPath.concat("/user/hadoop/tts_ods/tts_ods.bi_ods_cms_ips.log"))
      .filter(!_.isEmpty)
      .map(_.split(fieldTerminate))
      .filter(_.length >= 10)
      .map(p => (p(4).substring(0, 3), BiOdsCmsIps(p(4), p(5), if (p(9).length == 0) 0 else p(9).toLong)))
      .combineByKey(
        (v: BiOdsCmsIps) => List[BiOdsCmsIps](v),
        (c: List[BiOdsCmsIps], v: BiOdsCmsIps) => c.::(v),
        (c1: List[BiOdsCmsIps], c2: List[BiOdsCmsIps]) => c1.:::(c2)
      )

    //将IP数据放到Map中
    val ipMaps = ipRdd.collectAsMap()

    //save to oracle
    val output = new Path(hdfsPath.concat("/user/hadoop/tts_bi/behavior/_tmp_his_"))
    if (hdfs.exists(output)) hdfs.delete(output, true)

    bhvrHourly.map(_.split(fieldTerminate)).filter(_.length >= 27).coalesce(100, shuffle = true)
      .map(p => p(0) + fieldTerminate +
        p(1) + fieldTerminate +
        p(2) + fieldTerminate +
        p(3) + fieldTerminate +
        p(4) + fieldTerminate +
        p(5) + fieldTerminate +
        p(6) + fieldTerminate +
        p(8) + fieldTerminate +
        p(9) + fieldTerminate +
        p(10) + fieldTerminate +
        p(11) + fieldTerminate +
        p(12) + fieldTerminate +
        p(13) + fieldTerminate +
        p(26) + fieldTerminate +
        p(15) + fieldTerminate +
        p(16) + fieldTerminate +
        p(17) + fieldTerminate +
        p(20) + fieldTerminate +
        p(21) + fieldTerminate +
        p(23) + fieldTerminate +
        p(24) + fieldTerminate +
        p(25) + fieldTerminate +
        getCity(fixLength(p(15)), ipMaps) + fieldTerminate +
        p(19) + fieldTerminate +
        p(7) + fieldTerminate +
        p(14) + fieldTerminate +
        p(18) + fieldTerminate +
        p(19) + fieldTerminate +
        p(22))
      .coalesce(1, shuffle = true)
      .saveAsTextFile(hdfsPath.concat("/user/hadoop/tts_bi/behavior/_tmp_his_"))

    if (saveF == "Y") {
      //save to oracle
      val saveFOut = new Path(hdfsPath.concat("/user/hadoop/tts_bi/behavior/_tmp_his_2_"))
      if (hdfs.exists(output)) hdfs.delete(saveFOut, true)

      // 保存原数据
      val bhvrHourlyTmp = bhvrHourly.map(_.split(fieldTerminate)).filter(_.length >= 27).coalesce(100, shuffle = true)
        .map(p => p.mkString(fieldTerminate) + fieldTerminate + getCity(fixLength(p(15)), ipMaps))
      bhvrHourlyTmp.repartition(1).saveAsTextFile(hdfsPath.concat("/user/hadoop/tts_bi/behavior/_tmp_his_2_"))
    }

    sc.stop()
  }
}