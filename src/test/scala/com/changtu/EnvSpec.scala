package com.changtu

import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by lubinsu on 2016/6/13.
  * 检查环境配置是否正确
  */
class EnvSpec extends FlatSpec with Matchers{

  "Get environment" should "return confHome" in {
    val confHome = System.getenv("CONF_HOME")
    confHome should (be ("E:\\conf") or be ("/appl/conf"))
  }
}
