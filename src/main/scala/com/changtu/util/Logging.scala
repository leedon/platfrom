package com.changtu.util

import org.apache.log4j.{Logger, PropertyConfigurator}

/**
  * Created by lubinsu on 7/20/2016.
  *
  * 日志记录类
  */

trait Logging extends ClusterEnv {

  /**
    * log4j.properties属性文件配置
    *
    * @param s 文件所在路径
    */
  def setPropertyFile(s: String) {
    PropertyConfigurator.configure(s)
  }

  PropertyConfigurator.configure(confHome + "/log4j.properties")

  val logger = Logger.getLogger(getClass.getName)
}
