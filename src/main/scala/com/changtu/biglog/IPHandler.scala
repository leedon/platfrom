package com.changtu.biglog

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import play.api.libs.json.Json

import scala.io.Source

/**
  * IP处理
  * 调用方式：
  * spark-submit --class com.changtu.IPHandler
    --jars E:\IdeaProjects\jars\play-json_2.10-2.3.9.jar,E:\IdeaProjects\jars\hadoop-common-2.2.0.jar,E:\IdeaProjects\jars\play-functional_2.10-2.3.10.jar
    --master spark://tts.node4:7077
    E:\IdeaProjects\spark\out\artifacts\changtu\changtu.jar -1

    后续数据库导入命令：
    //导入语句
    sqoop export --connect jdbc:oracle:thin:@172.19.0.94:1521:orcl --username tts_bi --password tts_bi --table BI_ORDER_IPS --export-dir /user/root/order/orderIP --fields-terminated-by '\001' -m 1;
  */
object IPHandler {

  /**
    * 使用淘宝的REST接口获取IP数据, 返回JSON数据
    * @param ipAddr ip地址
    */
  def getIPJSON(ipAddr: String): String = {
    Source.fromURL("http://api.k780.com:88/?app=ip.get&ip=" concat ipAddr concat "&secret=5deac56db8f7d0547d2e6ef5e0acfd69&appkey=18553&sign=9b13fa5e333f9dcc11f74ff402f1b0d0&format=json").mkString
  }

  /**
    * 使用淘宝的REST接口获取IP数据, 返回运营商信息
    * @param ipAddr ip地址
    */
  def getISP(ipAddr: String): String = {
    val json = Json.parse(getIPJSON(ipAddr))
    val success: String = (json \ "success").as[String]
    if (success == "0") {
      (json \ "msg").as[String]
    } else {
      (json \ "result" \ "detailed").as[String]
    }
  }

  def main(args: Array[String]) {

    val hourDuration = args(0).toInt
    val fieldTerminate = "\001"
    val hdfsPath = "hdfs://172.19.0.95:9999"
    val hdfsURI = new URI(hdfsPath)
    val conf = new SparkConf().setAppName("FlumeNG sink")
    val sc = new SparkContext(conf)
    val hdfsConf = new Configuration()

    val srcFiles = hdfsPath.concat("/user/root/order/")
      .concat(DateTime.now().plusHours(hourDuration).toString("yyyyMMdd"))
      .concat("/order-")
      .concat(DateTime.now().plusHours(hourDuration).toString("yyyyMMddHH"))
      .concat(".[0-9]*.log")
    val srcFile = sc.textFile(srcFiles)
    val orderInfo = srcFile.map(line => line.split("\001")).filter(_.length > 11).filter(_ (0).contains("3011")).filter(!_ (11).isEmpty).map(x => (x(6), x(11), x(2).substring(x(2).indexOf("ip地址是") + 5)))
    val orderIP = orderInfo.map(x => x._1.concat(fieldTerminate).concat(x._2).concat(fieldTerminate).concat(x._3).concat(fieldTerminate).concat(getISP(x._3)).concat(fieldTerminate).concat(DateTime.now().toString("yyyy-MM-dd HH:mm:ss")))
    val output = new Path(hdfsPath.concat("/user/root/order/orderIP"))
    val hdfs = FileSystem.get(hdfsURI, hdfsConf)
    // 删除输出目录
    if (hdfs.exists(output)) hdfs.delete(output, true)
    orderIP.saveAsTextFile(hdfsPath.concat("/user/root/order/orderIP"))
  }
}
