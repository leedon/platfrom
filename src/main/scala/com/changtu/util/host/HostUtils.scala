package com.changtu.util.host

import scala.sys.process._

/**
  * Created by lubinsu on 8/8/2016.
  * 主机工具类
  */


object HostUtils {

  /**
    * 执行shell脚本
    * @param script 执行的脚本
    */
  def shell(script: String): Unit = {
    script.!
  }

}
