package com.changtu.util

/**
  * Created by lubinsu on 7/25/2016.
  *
  * 加载集群各类环境参数
  *
  */
trait ClusterEnv {

  val confHome = if (System.getenv("CONF_HOME") == "" || System.getenv("CONF_HOME") == null) "/appl/conf" else System.getenv("CONF_HOME")

}
