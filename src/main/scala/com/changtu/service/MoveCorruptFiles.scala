package com.changtu.service

import com.changtu.util.Logging
import com.changtu.util.hdfs.HDFSUtils
import org.apache.hadoop.fs.Path
import org.joda.time.DateTime

/**
  * Created by lubinsu on 7/25/2016.
  * 合并前文件检查，将tmp结尾的文件修改为log结尾的文件
  */
object MoveCorruptFiles extends Logging {

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: MoveCorruptFiles <srcDir> <duration>")
      System.exit(1)
    }

    val Array(path, duration) = args
    val hdfs = HDFSUtils.getHdfs

    val hdfsStatus = hdfs.listStatus(new Path(hdfs.getUri.toString.concat(path)))
    val date = DateTime.now().plusHours(duration.toInt).toString("yyyyMMddHH")
    val regex = ("(.*" + date + ".*\\.tmp)").r

    hdfsStatus.foreach(x => {
      x.getPath.getName match {
        case regex(url) =>
          HDFSUtils.mv(x.getPath.toUri.toString, x.getPath.toUri.toString.replace(".tmp", ""))
        case _ => ()
      }
    })
  }
}
