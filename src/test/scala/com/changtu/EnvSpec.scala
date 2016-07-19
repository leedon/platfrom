package com.changtu

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by lubinsu on 2016/6/13.
  * 检查环境配置是否正确
  */
class EnvSpec extends FlatSpec with Matchers {

  def rpad(s: String, length: Int, replace: String): String = {
    var rt = s
    while (rt.length() < length) rt = rt.concat(replace)
    rt
  }


  "Get environment" should "return confHome" in {
    val confHome = System.getenv("CONF_HOME")
    confHome should (be("E:\\conf") or be("/appl/conf"))
  }

  it should "judge whether started with the string" in {
    val filterStr = "1511,1566".split(",")
    filterStr.contains("1566")should be(true)
    val name = "biglog"
    name == "biglog" should be(true)

  }

  it should "get fix length of string" in {
    val str = rpad("#####", 10, "#")
    println(str)
    str should be("##########")
  }
}
