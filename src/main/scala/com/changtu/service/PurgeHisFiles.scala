package com.changtu.service

import com.changtu.util.Logging
import com.changtu.util.hdfs.HDFSUtils
import org.joda.time.DateTime

/**
  * Created by lubinsu on 7/25/2016.
  *
  * 删除历史数据，入参为文件路径和保留的天数
  * eg: 删除100天前的文件
  *
  * com.changtu.service.PurgeHisFiles /user/hadoop/querylog/{DATE_TIME}/ -7
  */
object PurgeHisFiles extends App with Logging{

  if (args.length < 2) {
    System.err.println("Usage: PurgeHisFiles <path> <hours>")
    System.exit(1)
  }

  val Array(path, hours) = args

  val delDir = path.replace("{DATE_TIME}", DateTime.now().plusDays(hours.toInt).toString("yyyyMMdd"))

  logger.info("Delete old dir ".concat(delDir))
  HDFSUtils.delete(delDir)

}
