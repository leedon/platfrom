package com.changtu.spark

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

    val sc = new SparkContext(new SparkConf().setAppName(name))

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
    sc.textFile(path)
  }

}

object SparkClient extends AbstractSparkClient {

}
