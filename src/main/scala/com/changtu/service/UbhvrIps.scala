package com.changtu.service

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
  * Created by lubinsu on 5/13/2016
  */
object UbhvrIps {

  case class BiOdsCmsIps(starIp: String, endIp: String, cityId: Long)

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


  def main(args: Array[String]) {

    val hourDuration = args(0).toInt
    val hdfsPath = "hdfs://nameservice1:8020"
    val hdfsURI = new URI(hdfsPath)
    val hdfsConf = new Configuration()
    val hdfs = FileSystem.get(hdfsURI, hdfsConf)
    //匹配文件名
    val filesNameFormat = "behavior-{DATE_TIME}.[0-9]*\\.log".replace("{DATE_TIME}", DateTime.now().plusHours(hourDuration).toString("yyyyMMddHH"))
    val srcFiles = hdfsPath.concat("/user/hadoop/behavior/hourly/").concat(filesNameFormat)
    val targetFile = hdfsPath.concat("/user/hadoop/behavior/").concat(DateTime.now().plusHours(hourDuration).toString("yyyyMMdd")).concat("/").concat(DateTime.now().plusHours(hourDuration).toString("HH"))

    val conf = new SparkConf().setAppName("UbhvrIps")
    val sc = new SparkContext(conf)

    val fieldTerminate = "\001"

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

    val output = new Path(targetFile)
    if (hdfs.exists(output)) hdfs.delete(output, true)

    //generate city_id
    //过滤掉字段长度超出数据库对应长度的数据
    val bhvrHourlyTmp = bhvrHourly.map(_.split(fieldTerminate)).filter(_.length >= 27)
      .filter(p => !(p(0).getBytes("GBK").length > 200
        || p(1).getBytes("GBK").length > 100
        || p(2).getBytes("GBK").length > 200
        || p(3).getBytes("GBK").length > 100
        || p(4).getBytes("GBK").length > 4000
        || p(5).getBytes("GBK").length > 4000
        || p(6).getBytes("GBK").length > 4000
        || p(7).getBytes("GBK").length > 100
        || p(8).getBytes("GBK").length > 100
        || p(9).getBytes("GBK").length > 100
        || p(10).getBytes("GBK").length > 100
        || p(11).getBytes("GBK").length > 100
        || p(12).getBytes("GBK").length > 100
        || p(13).getBytes("GBK").length > 100
        || p(14).getBytes("GBK").length > 100
        || p(15).getBytes("GBK").length > 100
        || p(16).getBytes("GBK").length > 100
        || p(17).getBytes("GBK").length > 4000
        || p(18).getBytes("GBK").length > 100
        || p(19).getBytes("GBK").length > 100
        || p(20).getBytes("GBK").length > 100
        || p(21).getBytes("GBK").length > 100
        || p(22).getBytes("GBK").length > 100
        || p(23).getBytes("GBK").length > 100
        || p(24).getBytes("GBK").length > 4000
        || p(25).getBytes("GBK").length > 4000
        || (if (p.length == 28) p(26) + p(27) else p(26)).getBytes("GBK").length > 4000)).coalesce(100, shuffle = true)
      .map(p => p.mkString(fieldTerminate) + fieldTerminate + getCity(fixLength(p(15)), ipMaps))
    //.coalesce(1, shuffle = true)
    //.saveAsTextFile(hdfsPath.concat("/user/hadoop/behavior/_tmp_his_"))

    //save to tmp directory for next step
    val outputTmp = new Path(hdfsPath.concat("/user/hadoop/tts_bi/behavior/_tmp_"))
    if (hdfs.exists(outputTmp)) hdfs.delete(outputTmp, true)
    bhvrHourlyTmp.map(_.split(fieldTerminate))
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
        (if (p.length == 29) p(26) + p(27) else p(26)) + fieldTerminate +
        p(15) + fieldTerminate +
        p(16) + fieldTerminate +
        p(17) + fieldTerminate +
        p(20) + fieldTerminate +
        p(21) + fieldTerminate +
        p(23) + fieldTerminate +
        p(24) + fieldTerminate +
        p(25) + fieldTerminate +
        (if (p.length == 29) p(28) else p(27)) + fieldTerminate +
        p(19) + fieldTerminate +
        p(7) + fieldTerminate +
        p(14) + fieldTerminate +
        p(18) + fieldTerminate +
        p(19) + fieldTerminate +
        p(22)).saveAsTextFile(hdfsPath.concat("/user/hadoop/tts_bi/behavior/_tmp_"))

    //save to hdfs
    bhvrHourlyTmp.repartition(1).saveAsTextFile(targetFile)

    //delete old files
    val regex = "(".concat(filesNameFormat).concat(")").r
    val hdfsStatus = hdfs.listStatus(new Path(hdfsPath.concat("/user/hadoop/behavior/hourly/")))
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