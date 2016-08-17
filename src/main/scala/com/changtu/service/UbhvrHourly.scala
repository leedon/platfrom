package com.changtu.service

import com.changtu.util.hdfs.HDFSUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
  * Created by lubinsu on 5/13/2016
  * 用户行为日志处理程序
  */
object UbhvrHourly {

  //IP
  case class BiOdsCmsIps(starIp: String, endIp: String, cityId: Long)

  //访问来源
  case class VisitSource(visitSourceId: Long, visitSourceName: String)


  /**
    * 格式化IP地址
    *
    * @param ip ip地址
    * @return 格式化后的IP
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

  /**
    * 获取IP对应的城市
    *
    * @param ipFormat 格式化IP
    * @param ipMap    IP段信息
    * @return 城市ID
    */
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

  /**
    * 获取来源ID
    *
    * @param visitSourceList 来源ID解释表
    * @param visitSource     用户行为记录的来源路径
    * @return 返回PK
    */
  def getVisitSourceId(visitSource: String, visitSourceList: List[VisitSource]): Long = {
    val sourceIds = visitSourceList.filter(p => visitSource.contains(p.visitSourceName)).map(_.visitSourceId)

    if (sourceIds.isEmpty) {
      99L
    } else sourceIds.max
  }

  def isDate(str: String, format: String): Boolean = {
    val formatter = DateTimeFormat.forPattern(format)
    try {
      val dateTime: DateTime = DateTime.parse(str, formatter)
      if (str.length == 23 || str.length == 0) {
        true
      } else false
    } catch {
      case ex: Exception =>
        if (str.trim.length == 0) {
          true
        } else false
    }
  }


  /**
    *
    * @param args src       需要处理的源文件目录，结尾必须加 / eg：/user/hadoop/behavior/hourly/
    *             dest      hdfs上合并完文件保存路径 eg：/user/hadoop/behavior/ 如果 incF = "Y" 则目录后面会自动添加日和小时
    *             或者 /user/hadoop/bigdata/output/
    *             tmpPath   处理好的临时数据，用于导入Oracle eg：/user/hadoop/tts_bi/behavior/_tmp_
    *             incF      是否是小时增量
    *             hourDuration 递增或者往前推移几个小时 eg：-1
    */
  def main(args: Array[String]) {

    if (args.length < 5) {
      System.err.println("Usage: UbhvrIps <src> <dest> <tmpPath> <incF> <hourDuration>")
      System.err.println("src       需要处理的源文件目录，结尾必须加 / eg：/user/hadoop/behavior/hourly/\ndest      hdfs上合并完文件保存路径 eg：/user/hadoop/behavior/ 如果 incF = \"Y\" 则目录后面会自动添加日和小时 或者 /user/hadoop/bigdata/output/\ntmpPath   处理好的临时数据，用于导入Oracle eg：/user/hadoop/tts_bi/behavior/_tmp_\nincF      是否是小时增量\nhourDuration 递增或者往前推移几个小时 eg：-1")
      System.err.println("spark-submit --master yarn-cluster --executor-memory 5G --executor-cores 3 --driver-memory 2G --conf spark.default.parallelism=30 --num-executors 5 --class com.changtu.service.UbhvrHourly /appl/scripts/e-business/platform/target/platform-1.1.jar  \"/user/hadoop/bigdata/test/\" \"/user/hadoop/bigdata/output/\" \"/user/hadoop/tts_bi/behavior/_tmp_\" \"N\" \"-1\"")
      System.exit(1)
    }

    // incF 是否增量数据，如果是(Y)增量数据的话，则根据时间增量匹配文件名
    val Array(src, dest, tmpPath, incF, hourDuration) = args
    val hdfs = HDFSUtils.getHdfs
    val hdfsPath = hdfs.getUri.toString

    //匹配文件名
    val filesNameFormat = incF match {
      case "Y" => "behavior-{DATE_TIME}.[0-9]*\\.log".replace("{DATE_TIME}", DateTime.now().plusHours(hourDuration.toInt).toString("yyyyMMddHH"))
      case "N" => "*"
    }

    val srcFiles = hdfsPath concat src concat filesNameFormat

    val targetFile = incF match {
      case "Y" => hdfsPath.concat(dest).concat(DateTime.now().plusHours(hourDuration.toInt).toString("yyyyMMdd")).concat("/").concat(DateTime.now().plusHours(hourDuration.toInt).toString("HH"))
      case "N" => dest
    }

    val srcBk = hdfsPath.concat("/user/hadoop/behavior/bk/").concat(DateTime.now().plusHours(hourDuration.toInt).toString("yyyyMMdd")).concat("/").concat(DateTime.now().plusHours(hourDuration.toInt).toString("HH"))

    val conf = new SparkConf().setAppName("com.changtu.service.UbhvrHourly")
    val sc = new SparkContext(conf)

    val fieldTerminate = "\001"

    //读取用户行为文件和 ip-city 映射数据
    val bhvrHourly = sc.textFile(srcFiles).filter(!_.isEmpty)
    if (incF == "Y") {
      //保留源数据，以做比对
      HDFSUtils.delete(srcBk)
      bhvrHourly.repartition(1).saveAsTextFile(srcBk)
    }


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

    // visitSource解释表
    val visitSourceList = sc.textFile(hdfsPath.concat("/user/hadoop/tts_ods/tts_ods.bi_ods_ecm_visit_sources.log"))
      .filter(!_.isEmpty)
      .map(_.split(fieldTerminate))
      .map(p => VisitSource(p(0).toLong, p(2))).collect.toList

    //将IP数据放到Map中
    val ipMaps = ipRdd.collectAsMap()

    // generate city_id
    // 过滤掉字段长度超出数据库对应长度的数据

    val bhvrHourlyTmp = bhvrHourly.map(_.split(fieldTerminate)).filter(_.length >= 36)
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
        //在此新增9个字段，来源于之前的visit表和query表
        || p(26).getBytes("GBK").length > 4000
        || p(27).getBytes("GBK").length > 30
        || !isDate(p(27), "yyyy-MM-dd HH:mm:ss:SSS")
        || p(28).getBytes("GBK").length > 20
        || p(29).getBytes("GBK").length > 100
        || p(30).getBytes("GBK").length > 200
        || p(31).getBytes("GBK").length > 30
        || p(32).getBytes("GBK").length > 30
        || p(33).getBytes("GBK").length > 300
        || p(34).getBytes("GBK").length > 100
        || (if (p.length == 37) p(35) + p(36) else p(35)).getBytes("GBK").length > 4000)).coalesce(100, shuffle = true)
      .map(p => p.mkString(fieldTerminate) + fieldTerminate +
        //客户端IP对应的城市Id
        getCity(fixLength(p(15)), ipMaps) + fieldTerminate +
        // visitSourceId
        getVisitSourceId(p(24), visitSourceList))

    //保存到临时文件夹
    HDFSUtils.delete(tmpPath)

    // val loc = new Locale("en")
    // 日期格式如：Wed Jul 27 12:38:20 CST 2016
    // val fm = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz y", loc)
    // val targetFm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", loc)

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
        (if (p.length == 39) p(35) + p(36) else p(35)) + fieldTerminate +
        p(15) + fieldTerminate +
        p(16) + fieldTerminate +
        p(17) + fieldTerminate +
        p(20) + fieldTerminate +
        (try {
          p(21).toLong
        } catch {
          case e: Exception => ""
        }).toString + fieldTerminate +
        p(23) + fieldTerminate +
        p(24) + fieldTerminate +
        p(25) + fieldTerminate +
        (if (p.length == 39) p(37) else p(36)) + fieldTerminate +
        p(19) + fieldTerminate +
        p(7) + fieldTerminate +
        p(14) + fieldTerminate +
        p(18) + fieldTerminate +
        p(19) + fieldTerminate +
        p(22) + fieldTerminate +
        p(26) + fieldTerminate +
        p(27) + fieldTerminate +
        p(28) + fieldTerminate +
        p(29) + fieldTerminate +
        p(30) + fieldTerminate +
        p(31) + fieldTerminate +
        p(32) + fieldTerminate +
        p(33) + fieldTerminate +
        p(34) + fieldTerminate +
        (if (p.length == 39) p(38) else p(37))).saveAsTextFile(hdfsPath.concat(tmpPath))

    // Save to hdfs
    HDFSUtils.delete(targetFile)
    bhvrHourlyTmp.repartition(1).saveAsTextFile(targetFile)

    //delete old files
    if (incF == "Y") {
      val fileStatus = hdfs.getFileStatus(new Path(targetFile))
      if (HDFSUtils.du(fileStatus) > 1024L) {
        //delete old files
        val regex = "(".concat(filesNameFormat).concat(")").r
        HDFSUtils.deleteRegex("/user/hadoop/behavior/hourly/", regex)
      }
    }

    sc.stop()
  }
}