package com.changtu.utils.spark

import com.changtu.utils.hdfs.HDFSClient
import com.twitter.logging.Logger
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lubinsu on 2016/6/12.
  * Spark基础功能类实现
  */
abstract class AbstractSparkClient {
  /**
    * 获取Spark上下文环境
    *
    * @param name application name
    * @return SparkContext
    */
  def getSparkContext(name: String): SparkContext = {

    val confHome = if (System.getenv("CONF_HOME") == "") "/appl/conf" else System.getenv("CONF_HOME")
    val sparkConf = new SparkConf().setAppName(name).setMaster("yarn-cluster")
    val sc = new SparkContext(sparkConf)

    sc.hadoopConfiguration.addResource(new Path(confHome + "/hdfs-site.xml"))
    sc.hadoopConfiguration.addResource(new Path(confHome + "/core-site.xml"))
    sc.hadoopConfiguration.addResource(new Path(confHome + "/yarn-site.xml"))
    sc.hadoopConfiguration.addResource(new Path(confHome + "/mapred-site.xml"))

    sc
  }

  /**
    * 获取来自hdfs的RDD
    *
    * @param sc   SparkContext
    * @param path 文件路径
    * @return HDFS RDD[String]
    */
  def getHadoopRDD(sc: SparkContext, path: String): RDD[String] = {

    sc.textFile(HDFSClient.hdfs.getUri.toString.concat(path))

  }

  def saveToHDFS[T](rdd: RDD[T]): Unit = {
    // rdd.saveAsTextFile()
  }

}

object SparkClient extends AbstractSparkClient {
  def main(args: Array[String]) {
    val log = Logger.get()
    val sc = SparkClient.getSparkContext("Test")
    log.info(SparkClient.getHadoopRDD(sc, "/user/hadoop/tts_ods/tts_ods.bi_ods_task_status.log").count().toString)
  }
}
