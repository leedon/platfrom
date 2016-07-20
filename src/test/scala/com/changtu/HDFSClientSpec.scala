package com.changtu

import com.changtu.utils.Logging
import com.changtu.utils.hdfs.HDFSClient
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by lubinsu on 2016/6/11.
  * 测试 HDFS client工具类
  */
class HDFSClientSpec extends FlatSpec with Matchers with Logging {

  val conf = new Configuration()

  // 加载HADOOP配置文件
  try {
    conf.addResource(new Path(confHome + "/hdfs-site.xml"))
    conf.addResource(new Path(confHome + "/core-site.xml"))
    conf.addResource(new Path(confHome + "/yarn-site.xml"))
    conf.addResource(new Path(confHome + "/mapred-site.xml"))
  } catch {
    case e: IllegalArgumentException =>
      conf.addResource(new Path(confHome + "/hdfs-site.xml"))
      conf.addResource(new Path(confHome + "/core-site.xml"))
      conf.addResource(new Path(confHome + "/yarn-site.xml"))
      conf.addResource(new Path(confHome + "/mapred-site.xml"))
  }

  val hdfs = FileSystem.get(conf)

  "HDFS client" should "create a file" in {
    HDFSClient.createDirectory("/user/hadoop/test", deleteF = true) should be(true)
    HDFSClient.release()
  }

  it should "return the file's info " in {

    val p = new Path("hdfs://nameservice1/user/hadoop/behavior/20160719/")
    val fileStatus = hdfs.getFileStatus(p)

    val totalSize = HDFSClient.du(fileStatus)
    logger.info("文件路径：" + fileStatus.getPath)
    logger.info("块的大小：" + fileStatus.getBlockSize)
    logger.info("文件所有者：" + fileStatus.getOwner + ":" + fileStatus.getGroup)
    logger.info("文件权限：" + fileStatus.getPermission)
    logger.info("文件长度：" + (totalSize.toDouble / 1024 / 1024).round + " M")
    logger.info("文件长度：" + totalSize + " Bytes")
    logger.info("备份数：" + fileStatus.getReplication)
    logger.info("修改时间：" + fileStatus.getModificationTime)
    totalSize should be(2895130002L)

  }

  it should "return the right hdfs uri" in {
    HDFSClient.getHdfs.getUri.toString should be("hdfs://nameservice1")
    val hdfsStatus = hdfs.listStatus(new Path(hdfs.getUri.toString.concat("/user/hadoop/behavior/20160720/")))
    hdfsStatus.foreach(p => println(p.getPath.getName))
  }
}
