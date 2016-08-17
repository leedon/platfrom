package com.changtu

import com.changtu.util.Logging
import com.changtu.util.host.HostUtils
import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by lubinsu on 2016/6/13.
  * 检查环境配置是否正确
  */
class EnvSpec extends FlatSpec with Matchers with Logging {

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

  it should "return regex str" in {
    val url = "/ticket/querySchListOnPage.htm?endTypeId=2&endId=410283&planDate=2016-07-19&startCityUrl=hangzhoushi&endCityUrl=&page=1&localStartStations=&stopNames=&planTimes=&sort=&localYes=&preF=0"
    val regex = "(startCityUrl=\\w*&)".r
    logger.info((regex findFirstIn url).getOrElse("").replace("startCityUrl=", "").replace("&", ""))
  }

  it should "return the right incF" in {

    val incF = "Y"
    val filesNameFormat = incF match {
      case "Y" => "Y"
      case "N" => "N"
    }

    filesNameFormat should be ("Y")
  }
  it should "return the formated time" in {

    logger.info(DateTime.now().toString("yyyyMMddHHmmss"))
  }

  it should "return the right env" in {
    val map = System.getenv()
    logger.info(map.toString)

    
  }
}
