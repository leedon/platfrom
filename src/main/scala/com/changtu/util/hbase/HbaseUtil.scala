package com.changtu.util.hbase

import java.util.concurrent.Executors

import com.changtu.util.ClusterEnv
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

/**
  * Created by lubinsu on 7/28/2016.
  */
object HbaseUtil extends Serializable with ClusterEnv {

  private val conf = HBaseConfiguration.create()

  try {
    conf.addResource(new Path(confHome + "/hbase-site.xml"))
    conf.addResource(new Path(confHome + "/core-site.xml"))
  } catch {
    case e: IllegalArgumentException =>
      conf.addResource(new Path(confHome + "/hbase-site.xml"))
      conf.addResource(new Path(confHome + "/core-site.xml"))
  }

  def apply(threadCount: Int = 1): Connection = {
    val executor = Executors.newFixedThreadPool(threadCount)
    ConnectionFactory.createConnection(conf, executor)
  }

  /*val executor = Executors.newFixedThreadPool(10)
  private val connection = ConnectionFactory.createConnection(conf, executor)*/
  def getHbaseConn: Connection = {
    val connection = ConnectionFactory.createConnection(conf)
    connection
  }
}
