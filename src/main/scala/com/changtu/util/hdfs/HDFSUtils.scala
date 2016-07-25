package com.changtu.util.hdfs

/**
  * Created by lubinsu on 2016/6/8.
  * HDFS工具类
  */

import com.changtu.util.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, _}

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

abstract class AbstractFSClient extends Logging {
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

  def getHdfs = hdfs
}

object HDFSUtils extends AbstractFSClient {

  /**
    * 获取目录下的所有文件
    *
    * @param fileStatus 目录
    * @param listStatus 返回文件清单
    */
  def showFiles(fileStatus: FileStatus, listStatus: ListBuffer[FileStatus]): Unit = {
    if (fileStatus.isDirectory) {
      val fs = hdfs.listStatus(fileStatus.getPath)
      if (fs.nonEmpty) {
        for (f <- fs) {
          showFiles(f, listStatus)
        }
      }
    } else if (fileStatus.isFile) {
      listStatus.+=(fileStatus)
    }
  }

  def mv(src: String, dest: String): Boolean = {
    logger.info("moving ".concat(src).concat(" to ").concat(dest))
    hdfs.rename(new Path(src), new Path(dest))
  }

  /**
    * 获取目录下文件总大小（单副本的大小） disk usage
    *
    * @param fileStatus 输入文件路径
    * @return 返回文件大小 Byte
    */
  def du(fileStatus: FileStatus): Long = {
    val listStatus: ListBuffer[FileStatus] = ListBuffer()
    showFiles(fileStatus, listStatus)
    listStatus.map(p => p.getLen).sum
  }

  /**
    * delete hdfs files
    *
    * @param path      path to be deleted
    * @param recursive if path is a directory and set to
    *                  true, the directory is deleted else throws an exception. In
    *                  case of a file the recursive can be set to either true or false.
    * @return true if delete is successful else false.
    */
  def delete(path: String, recursive: Boolean = true): Boolean = {

    val output = new Path(path)
    if (hdfs.exists(output)) {
      val flag = hdfs.delete(output, recursive)
      flag
    } else {
      true
    }
  }

  /**
    * delete hdfs files
    *
    * @param path      path to be deleted
    * @param recursive if path is a directory and set to
    *                  true, the directory is deleted else throws an exception. In
    *                  case of a file the recursive can be set to either true or false.
    * @return true if delete is successful else false.
    */
  def delete(path: Path, recursive: Boolean): Boolean = {

    if (hdfs.exists(path)) {
      val flag = hdfs.delete(path, recursive)
      flag
    } else {
      true
    }
  }

  /**
    * 删除匹配正则表达式的文件
    *
    * @param path  需要匹配删除的目录
    * @param regex 正则
    */
  def deleteRegex(path: String, regex: Regex): Unit = {
    val hdfsStatus = hdfs.listStatus(new Path(hdfs.getUri.toString.concat(path)))
    hdfsStatus.foreach(x => {
      x.getPath.getName match {
        case regex(url) =>
          logger.info("Deleting file : " + url)
          delete(x.getPath, recursive = true)
        case _ => ()
      }
    })
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
