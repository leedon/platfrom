package com.changtu.service

import com.changtu.util.Logging
import com.changtu.util.hdfs.HDFSUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
  * Created by lubinsu on 2016/4/26
  *
  * 合并数据文件功能
  */
object PutMerge extends Logging {

  /**
    * 设定字符串的长度，不足的以特定字符补齐
    *
    * @param s       输入的字符串
    * @param length  设定长度
    * @param replace 填充字符
    * @return 返回补齐后的字符串
    */
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
    val fieldTerminate = "\001"
    val hdfs = HDFSUtils.getHdfs
    val hdfsPath = hdfs.getUri.toString
    val conf = new SparkConf().setAppName("Put merge")
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

      val biglogFile = hdfsPath.concat(targetDir).concat(DateTime.now().plusHours(hourDuration).toString("yyyyMMdd")).concat("/").concat(DateTime.now().plusHours(hourDuration).toString("HH"))
      val queryLogFile = hdfsPath.concat("/user/hadoop/querylog/").concat(DateTime.now().plusHours(hourDuration).toString("yyyyMMdd")).concat("/").concat(DateTime.now().plusHours(hourDuration).toString("HH"))
      val srcFileRDD = sc.textFile(srcFiles)

      // delete directory
      HDFSUtils.delete(biglogFile)
      HDFSUtils.delete(queryLogFile)

      logger.info("Merging files ")
      srcFileRDD.map(_.split(fieldTerminate)).filter(p => filterStr.contains(p(0))).filter(p => p.length > 1).map(p => p.mkString(fieldTerminate).concat(rpad(fieldTerminate, 22 - p.length, fieldTerminate))).repartition(1).saveAsTextFile(queryLogFile)
      srcFileRDD.map(_.split(fieldTerminate)).filter(p => !filterStr.contains(p(0))).filter(p => p.length > 1).map(p => p.mkString(fieldTerminate).concat(rpad(fieldTerminate, 22 - p.length, fieldTerminate))).repartition(1).saveAsTextFile(biglogFile)

    } else {
      val targetFile = hdfsPath.concat(targetDir).concat(DateTime.now().plusHours(hourDuration).toString("yyyyMMdd")).concat("/").concat(DateTime.now().plusHours(hourDuration).toString("HH"))
      val srcFileRDD = sc.textFile(srcFiles)

      HDFSUtils.delete(targetFile)

      logger.info("Merging files ")
      srcFileRDD.repartition(1).saveAsTextFile(targetFile)
    }

    //检查文件是否正常生成了
    val p = new Path(hdfsPath.concat(targetDir).concat(DateTime.now().plusHours(hourDuration).toString("yyyyMMdd")).concat("/").concat(DateTime.now().plusHours(hourDuration).toString("HH")))
    val fileStatus = hdfs.getFileStatus(p)
    if (HDFSUtils.du(fileStatus) > 1024L) {
      //delete old files
      val regex = "(".concat(filesNameFormat).concat(")").r
      HDFSUtils.deleteRegex(srcDir, regex)
    }

    sc.stop()
  }
}