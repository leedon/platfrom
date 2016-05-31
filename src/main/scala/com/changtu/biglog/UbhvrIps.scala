package com.changtu.biglog

import java.net.URI
import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}
import java.text.SimpleDateFormat

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

import scala.io.Source

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

  //将数据加载到Oracle
  /*def toOracle(iterator: Iterator[Array[String]], props: Map[String, String], ipMaps: scala.collection.Map[String, List[BiOdsCmsIps]]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "INSERT INTO bi_ods_user_bhvr_ips\n  (pk_user_analysis_date,\n   unique_id,\n   user_id,\n   type_cd,\n   log_url,\n   log_his_refer,\n   title,\n   charset,\n   log_platform,\n   cookie_enabled,\n   os_language,\n   sys_language,\n   sr,\n   VALUE,\n   client_ip,\n   server_record_time,\n   user_agent,\n   browser,\n   fk_user_id,\n   page_id,\n   visit_source,\n   visit_path,\n   city_id,\n   create_date,\n   \n   modified_date,\n   TIME,\n   transfered_date,\n   complete_date,\n   static_date)\nVALUES\n  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    try {
      conn = DriverManager.getConnection(props.getOrElse("jdbc.url.ods", "jdbc:oracle:thin:@172.19.0.94:1521:orcl"),
        props.getOrElse("jdbc.username.ods", "tts_ods"),
        props.getOrElse("jdbc.password.ods", "tts_ods"))

      iterator.foreach(p => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, p(0))
        ps.setString(2, p(1))
        ps.setString(3, p(2))
        ps.setString(4, p(3))
        ps.setString(5, p(4))
        ps.setString(6, p(5))
        ps.setString(7, p(6))
        ps.setString(8, p(8))
        ps.setString(9, p(9))
        ps.setString(10, p(10))
        ps.setString(11, p(11))
        ps.setString(12, p(12))
        ps.setString(13, p(13))
        ps.setString(14, p(26))
        ps.setString(15, p(15))
        ps.setString(16, p(16))
        ps.setString(17, p(17))
        ps.setString(18, p(20))
        ps.setString(19, p(21))
        ps.setString(20, p(23))
        ps.setString(21, p(24))
        ps.setString(22, p(25))
        ps.setString(23, getCity(fixLength(p(15)), ipMaps))
        ps.setTimestamp(24, new Timestamp(new SimpleDateFormat("yyyy-MM-dd H:mm:ss:S").parse(p(19)).getTime))
        ps.executeUpdate()
        ps.close()
      }
      )
    } catch {
      case e: Exception => throw e
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }*/

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

  def ips(x: String, y: String): String = {
    ""
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
    val bhvrHourlyTmp = bhvrHourly.map(_.split(fieldTerminate)).filter(_.length >= 27).coalesce(100, shuffle = true)
      .map(p => p.mkString(fieldTerminate) + fieldTerminate + getCity(fixLength(p(15)), ipMaps))
    //.coalesce(1, shuffle = true)
    //.saveAsTextFile(hdfsPath.concat("/user/hadoop/behavior/_tmp_his_"))

    //save to tmp directory for next step
    val outputTmp = new Path(hdfsPath.concat("/user/hadoop/behavior/_tmp_"))
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
        p(26) + fieldTerminate +
        p(15) + fieldTerminate +
        p(16) + fieldTerminate +
        p(17) + fieldTerminate +
        p(20) + fieldTerminate +
        p(21) + fieldTerminate +
        p(23) + fieldTerminate +
        p(24) + fieldTerminate +
        p(25) + fieldTerminate +
        p(27) + fieldTerminate +
        p(19) + fieldTerminate +
        p(7) + fieldTerminate +
        p(14) + fieldTerminate +
        p(18) + fieldTerminate +
        p(19) + fieldTerminate +
        p(22)).saveAsTextFile(hdfsPath.concat("/user/hadoop/behavior/_tmp_"))

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