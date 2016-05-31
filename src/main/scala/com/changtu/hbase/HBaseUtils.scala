package com.changtu.hbase

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

/**
  * Created by lubinsu on 2016/3/31
  *
  *
--class
    com.changtu.hbase.HBaseUtils
    --jars
    E:\IdeaProjects\jars\guava-12.0.1.jar,E:\IdeaProjects\jars\hbase-annotations-1.1.3.jar,E:\IdeaProjects\jars\hbase-annotations-1.1.3-tests.jar,E:\IdeaProjects\jars\hbase-client-1.1.3.jar,E:\IdeaProjects\jars\hbase-common-1.1.3.jar,E:\IdeaProjects\jars\hbase-common-1.1.3-tests.jar,E:\IdeaProjects\jars\hbase-examples-1.1.3.jar,E:\IdeaProjects\jars\hbase-hadoop2-compat-1.1.3.jar,E:\IdeaProjects\jars\hbase-hadoop-compat-1.1.3.jar,E:\IdeaProjects\jars\hbase-it-1.1.3.jar,E:\IdeaProjects\jars\hbase-it-1.1.3-tests.jar,E:\IdeaProjects\jars\hbase-prefix-tree-1.1.3.jar,E:\IdeaProjects\jars\hbase-procedure-1.1.3.jar,E:\IdeaProjects\jars\hbase-protocol-1.1.3.jar,E:\IdeaProjects\jars\hbase-resource-bundle-1.1.3.jar,E:\IdeaProjects\jars\hbase-rest-1.1.3.jar,E:\IdeaProjects\jars\hbase-server-1.1.3.jar,E:\IdeaProjects\jars\hbase-server-1.1.3-tests.jar,E:\IdeaProjects\jars\hbase-shell-1.1.3.jar,E:\IdeaProjects\jars\hbase-thrift-1.1.3.jar,E:\IdeaProjects\jars\htrace-core-3.1.0-incubating.jar
    --master
    spark://tts.node4:7077
    E:\IdeaProjects\spark\out\artifacts\changtu\changtu.jar
  */
object HBaseUtils {


  def main(args: Array[String]) {

    val config = HBaseConfiguration.create()

    config.addResource(new Path("E:\\documents\\conf\\hbase-site.xml"))
    config.addResource(new Path("E:\\documents\\conf\\core-site.xml"))

    val connection = ConnectionFactory.createConnection(config)
    val admin = connection.getAdmin


    val tableName = TableName.valueOf("bi_user_label_string")
    val table = connection.getTable(tableName)
    if (admin.tableExists(tableName)) {
      println("Table exist.")
    } else {
      println("Table does not exist.")
      System.exit(-1)
    }

    val result = table.get(new Get(Bytes.toBytes("1735909")))
    println(Bytes.toString(result.value()))
    admin.close()
  }
}