package com.changtu.biglog

import java.net.URI
import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import play.api.libs.json.Json

import scala.io.Source

/**
  * Created by lubinsu on 2016/3/29
  */
object PcSaveOrder {

  //创建对象
  case class BiOdsPcSaveOrder(orderId: Long, flowCode: String, orderUserId: Long, startCityId: Long, endCityId: Long, ip: String, orderCount: Long, arMoney: Float, planDate: String, flag: String)

  case class BiOdsPcSaveOrders(orderId: String, flowCode: String, orderUserId: String, startCityId: String, endCityId: String, ip: String, orderCount: String, arMoney: String, planDate: String, flag: String)

  //将数据加载到Oracle
  def toOracle(iterator: Iterator[(BiOdsPcSaveOrder)], props: Map[String, String]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "INSERT INTO bi_ods_pc_save_order\n  (order_id,\n   flow_code,\n   order_user_id,\n   start_city_id,\n   end_city_id,\n   ip,\n   order_count,\n   ar_money,\n   plan_date, flag)\nVALUES\n  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    try {
      conn = DriverManager.getConnection(props.getOrElse("jdbc.url.ods", "jdbc:oracle:thin:@172.19.0.94:1521:orcl"),
        props.getOrElse("jdbc.username.ods", "tts_ods"),
        props.getOrElse("jdbc.password.ods", "tts_ods"))

      iterator.foreach(data => {
        ps = conn.prepareStatement(sql)
        ps.setLong(1, data.orderId)
        ps.setString(2, data.flowCode)
        ps.setLong(3, data.orderUserId)
        ps.setLong(4, data.startCityId)
        ps.setLong(5, data.endCityId)
        ps.setString(6, data.ip)
        ps.setLong(7, data.orderCount)
        ps.setFloat(8, data.arMoney)
        ps.setString(9, data.planDate)
        ps.setString(10, data.flag)
        ps.executeUpdate()
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

  // Main函数
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("biglog analyze")
    val sc = new SparkContext(conf)

    val fieldTerminate = "\001"
    val hdfsPath = "hdfs://nameservice1:8020"
    val props = getJdbcProps
    val hdfsConf = new Configuration()
    val hdfs = FileSystem.get(new URI(hdfsPath), hdfsConf)


    //正则匹配小时文件夹内生成的数据文件
    val regex = "(biglog-[0-9]*.[0-9]*.log)".r
    val hdfsStatus = hdfs.listStatus(new Path(hdfsPath.concat("/user/hadoop/biglog/hourly")))

    //初始化源文件清单
    var srcFiles = sc.parallelize(List(""))

    //获取已经抽取过的数据清单
    var fileList = sc.textFile(hdfsPath.concat("/tmp/logs/bi_ods_pc_save_order.filelist"))
    hdfsStatus.foreach(p => {
      p.getPath.getName match {
        case regex(name) =>
          if (fileList.filter(_.contains(name)).count() == 0) {
            //如果文件未通不过，那么加载该文件
            println("Load File :" + name)
            srcFiles = srcFiles.union(sc.textFile(hdfsPath.concat("/user/hadoop/biglog/hourly/").concat(name)))
            fileList = fileList.++(sc.parallelize(List(name)))
          }
        case _ => // do nothing
      }
    })

    val bigLogRDD = srcFiles.filter(!_.isEmpty).map(_.split(fieldTerminate, 22)).filter(_.length == 22)
    //只取提交的订单
    val bigLogPcSaveOrder = bigLogRDD.filter(p => p(0).contains("2024")).filter(!_ (3).contains("没有提交订单"))

    //将数据处理成对应的数据模型
    val bigLogPcSaveOrderTab1 = bigLogPcSaveOrder.map(p => BiOdsPcSaveOrders(
      if (p(4).length == 0) "" else (Json.parse(p(4)) \ "orderIds" \\ "orderId").map(_.as[String]).mkString(","),
      if (p(1).length == 0) "" else p(1),
      (Json.parse(p(3)) \ "orderUserId").as[String],
      (Json.parse(p(3)) \ "ordersJson").asOpt[String] match { case Some(a) => (Json.parse(a) \ "orders" \\ "startCityId").map(_.as[String]).mkString(",") case None => "" },
      (Json.parse(p(3)) \ "ordersJson").asOpt[String] match { case Some(a) => (Json.parse(a) \ "orders" \\ "endId").map(_.as[String]).mkString(",") case None => "" },
      p(5),
      (Json.parse(p(3)) \ "ordersJson").asOpt[String] match { case Some(a) => (Json.parse(a) \ "orders" \\ "orderCount").map(_.as[Long]).mkString(",") case None => "" },
      (Json.parse(p(3)) \ "ordersJson").asOpt[String] match { case Some(a) => (Json.parse(a) \ "orders" \\ "arMoney").map(_.as[Float]).mkString(",") case None => "" },
      (Json.parse(p(3)) \ "ordersJson").asOpt[String] match { case Some(a) => (Json.parse(a) \ "orders" \\ "planDate").map(_.as[String]).mkString(",") case None => "" },
      if (p(4).length == 0) "" else (Json.parse(p(4)) \ "flag").as[String]))

    val bigLogPcSaveOrderTab = bigLogPcSaveOrderTab1.map(p => {
      var i = 0
      val len = p.orderId.split(",").length
      var orders = ""
      while (i < len) {
        if (i == 0) {
          orders = p.orderId.split(",")(i) + "\002" + p.flowCode.toString + "\002" + p.orderUserId.toString + "\002" + p.startCityId.split(",")(i) + "\002" +
            p.endCityId.split(",")(i) + "\002" + p.ip + "\002" + p.orderCount.split(",")(i) + "\002" +
            p.arMoney.split(",")(i) + "\002" + p.planDate.split(",")(i) + "\002" + p.flag
        } else {
          orders = p.orderId.split(",")(i) + "\002" + p.flowCode.toString + "\002" + p.orderUserId.toString + "\002" + p.startCityId.split(",")(i) + "\002" +
            p.endCityId.split(",")(i) + "\002" + p.ip + "\002" + p.orderCount.split(",")(i) + "\002" +
            p.arMoney.split(",")(i) + "\002" + p.planDate.split(",")(i) + "\002" + p.flag + "\001" + orders
        }
        i += 1
      }
      orders
    }).flatMap(p => p.split(fieldTerminate)).map(p => p.split("\002")).map(p => BiOdsPcSaveOrder(
      if (p(0).trim.length == 0) 0 else p(0).toLong,
      p(1),
      if (p(2).trim.length == 0) 0 else p(2).toLong,
      if (p(3).trim.length == 0) 0 else p(3).toLong,
      if (p(4).trim.length == 0) 0 else p(4).toLong,
      p(5),
      if (p(6).trim.length == 0) 0 else p(6).toLong,
      if (p(7).trim.length == 0) 0 else p(7).toFloat,
      p(8),
      p(9)))

    //保存数据
    bigLogPcSaveOrderTab.foreachPartition(toOracle(_, props))

    //将已经同步过的文件记录下来
    fileList.repartition(1).saveAsTextFile(hdfsPath.concat("/tmp/logs/bi_ods_pc_save_order.filelist.tmp"))
    val fileListPath = new Path(hdfsPath.concat("/tmp/logs/bi_ods_pc_save_order.filelist"))
    if (hdfs.exists(fileListPath)) hdfs.delete(fileListPath, true)
    sc.textFile(hdfsPath.concat("/tmp/logs/bi_ods_pc_save_order.filelist.tmp")).repartition(1).saveAsTextFile(hdfsPath.concat("/tmp/logs/bi_ods_pc_save_order.filelist"))
    if (hdfs.exists(new Path(hdfsPath.concat("/tmp/logs/bi_ods_pc_save_order.filelist.tmp")))) hdfs.delete(new Path(hdfsPath.concat("/tmp/logs/bi_ods_pc_save_order.filelist.tmp")), true)
  }
}