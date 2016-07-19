package com.changtu.biglog

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
  * Created by lubinsu on 2016/4/26
  */
object TempProc {

  def rpad(s: String, length: Int, replace: String): String = {
    var rt = s
    while (rt.length() < length) rt = rt.concat(replace)
    rt
  }

  def main(args: Array[String]) {

    if (args.length < 6) {
      System.err.println("Usage: PutMerge <srcDir> <filesNameFormat> <targetDir> <hourDuration> <name> <filterStr>")
      System.exit(1)
    }

    //namenode uri
    val hdfsPath = "hdfs://nameservice1:8020"
    val fieldTerminate = "\001"
    val hdfsURI = new URI(hdfsPath)
    val hdfsConf = new Configuration()
    val hdfs = FileSystem.get(hdfsURI, hdfsConf)
    val conf = new SparkConf().setAppName("put merge")
    val sc = new SparkContext(conf)
    val srcDir = args(0)
    val targetDir = args(2)
    val hourDuration = args(3).toInt

    // 需要过滤的第一个字段的值，逗号分隔
    val name = args(4)
    val filterStr = args(5).split(",")
    val filesNameFormat = args(1).replace("{DATE_TIME}", DateTime.now().plusHours(hourDuration).toString("yyyyMMddHH"))

    val srcFiles = hdfsPath.concat(srcDir).concat(filesNameFormat)

    // 如果是针对大日志的合并，则将查询接口
    if (name == "biglog") {

      println("biglog")
      val biglogFile = hdfsPath.concat(targetDir).concat(DateTime.now().plusHours(hourDuration).toString("yyyyMMdd")).concat("/").concat(DateTime.now().plusHours(hourDuration).toString("HH"))
      val queryLogFile = hdfsPath.concat("/user/hadoop/querylog/").concat(DateTime.now().plusHours(hourDuration).toString("yyyyMMdd")).concat("/").concat(DateTime.now().plusHours(hourDuration).toString("HH"))
      val srcFileRDD = sc.textFile(srcFiles)

      val biglogOutput = new Path(biglogFile)
      if (hdfs.exists(biglogOutput)) hdfs.delete(biglogOutput, true)

      val queryLogOutput = new Path(queryLogFile)
      if (hdfs.exists(queryLogOutput)) hdfs.delete(queryLogOutput, true)

      println("merging files ")
      srcFileRDD.map(_.split(fieldTerminate)).filter(p => filterStr.contains(p(0))).filter( p => p.length > 1).map(p => p.mkString(fieldTerminate).concat(rpad(fieldTerminate, 22 - p.length, fieldTerminate))).repartition(1).saveAsTextFile(queryLogFile)
      srcFileRDD.map(_.split(fieldTerminate)).filter(p => !filterStr.contains(p(0))).filter( p => p.length > 1).map(p => p.mkString(fieldTerminate).concat(rpad(fieldTerminate, 22 - p.length, fieldTerminate))).repartition(1).saveAsTextFile(biglogFile)

    } else {
      val targetFile = hdfsPath.concat(targetDir).concat(DateTime.now().plusHours(hourDuration).toString("yyyyMMdd")).concat("/").concat(DateTime.now().plusHours(hourDuration).toString("HH"))
      val srcFileRDD = sc.textFile(srcFiles)
      val output = new Path(targetFile)

      if (hdfs.exists(output)) hdfs.delete(output, true)

      println("merging files ")
      srcFileRDD.repartition(1).saveAsTextFile(targetFile)
    }


    //delete old files
    /*val regex = "(".concat(filesNameFormat).concat(")").r
    val hdfsStatus = hdfs.listStatus(new Path(hdfsPath.concat(srcDir)))
    hdfsStatus.foreach(x => {
      x.getPath.getName match {
        case regex(url) =>
          println("deleting file : " + url)
          hdfs.delete(x.getPath, true)
        case _ => ()
      }
    })*/

    sc.stop()
  }
}