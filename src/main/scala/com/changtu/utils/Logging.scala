package com.changtu.utils

import org.apache.log4j.{Logger, PropertyConfigurator}

/**
  * Created by lubinsu on 7/20/2016.
  *
  * 日志记录类
  */

trait Logging {

  /**
    * log4j.properties属性文件配置
    * @param s 文件所在路径
    */
  def setPropertyFile(s: String) {
    PropertyConfigurator.configure(s)
  }

  val confHome = if (System.getenv("CONF_HOME") == "" || System.getenv("CONF_HOME") == null) "/appl/conf" else System.getenv("CONF_HOME")
  PropertyConfigurator.configure(confHome + "/log4j.properties")

  val logger = Logger.getLogger(getClass.getName)
}
