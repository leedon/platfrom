package com.changtu.pub

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
  * Created by lubinsu on 2016/4/26
  */
object PutMerge {


  def main(args: Array[String]) {
    //namenode uri
    val hdfsPath = "hdfs://nameservice1:8020"
    val hdfsURI = new URI(hdfsPath)
    val hdfsConf = new Configuration()
    val hdfs = FileSystem.get(hdfsURI, hdfsConf)
    val conf = new SparkConf().setAppName("FlumeNG sink")
    val sc = new SparkContext(conf)
    val srcDir = args(0)
    val targetDir = args(2)
    val hourDuration = args(3).toInt
    val filesNameFormat = args(1).replace("{DATE_TIME}", DateTime.now().plusHours(hourDuration).toString("yyyyMMddHH"))

    val srcFiles = hdfsPath.concat(srcDir).concat(filesNameFormat)
    val targetFile = hdfsPath.concat(targetDir).concat(DateTime.now().plusHours(hourDuration).toString("yyyyMMdd")).concat("/").concat(DateTime.now().plusHours(hourDuration).toString("HH"))
    // hdfs://nameservice1:8020/user/hadoop/biglog/hourly/biglog-2016042808.[0-9]*.log
    val srcFileRDD = sc.textFile(srcFiles)
    val output = new Path(targetFile)

    if (hdfs.exists(output)) hdfs.delete(output, true)
    println("merging files ")
    srcFileRDD.repartition(1).saveAsTextFile(targetFile)

    //delete old files
    val regex = "(".concat(filesNameFormat).concat(")").r
    val hdfsStatus =  hdfs.listStatus(new Path(hdfsPath.concat(srcDir)))
    hdfsStatus.foreach( x => {
      x.getPath.getName match {
        case regex(url) => {
          println("deleting file : " + url)
          hdfs.delete(x.getPath, true)
        }
        case _ => ()
      }
    })
  }
}
