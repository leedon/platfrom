package com.changtu.hdfs

/**
  * Created by lubinsu on 2016/6/8.
  */

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, _}

abstract class AbstractFSClient {

}

object HDFSClient extends AbstractFSClient {
  val conf = new Configuration()
  // 加载HADOOP配置文件
  try {
    conf.addResource(new Path("E:\\conf\\hdfs-site.xml"))
    conf.addResource(new Path("E:\\conf\\core-site.xml"))
    conf.addResource(new Path("E:\\conf\\yarn-site.xml"))
    conf.addResource(new Path("E:\\conf\\mapred-site.xml"))
  } catch {
    case e: IllegalArgumentException =>
      conf.addResource(new Path("/appl/conf/hdfs-site.xml"))
      conf.addResource(new Path("/appl/conf/core-site.xml"))
      conf.addResource(new Path("/appl/conf/yarn-site.xml"))
      conf.addResource(new Path("/appl/conf/mapred-site.xml"))
  }

  val hdfs = FileSystem.get(conf)

  /**
    * delete hdfs files
    *
    * @param path      path to be deleted
    * @param recursive if path is a directory and set to
    *                  true, the directory is deleted else throws an exception. In
    *                  case of a file the recursive can be set to either true or false.
    * @return true if delete is successful else false.
    */
  def delete(path: String, recursive: Boolean): Boolean = {

    val output = new Path(path)
    if (hdfs.exists(output)) {
      val flag = hdfs.delete(output, recursive)
      flag
    } else {
      true
    }
  }

  /**
    * Create a directory or file
    *
    * @param path    the path to be created
    * @param deleteF whether delete the dir if exists
    * @return true if create is successful else false.
    */
  def createDirectory(path: String, deleteF: Boolean): Boolean = {

    val output = new Path(path)

    if (hdfs.exists(output) && deleteF) {
      delete(path, recursive = true)
      hdfs.create(output)
    } else hdfs.create(output)

    if (hdfs.exists(output)) {
      true
    } else {
      false
    }
  }

  def release(): Unit = {
    hdfs.close()
  }
}
