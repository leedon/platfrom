package com.changtu

import com.twitter.logging.Logger
import org.scalatest.{Matchers, FlatSpec}
import play.api.libs.json.Json

/**
  * Created by lubinsu on 2016/6/14.
  */
class JsonSpec extends FlatSpec with Matchers {

  val log = Logger.get()

  "Json parser" should " get \"1\"" in {

    val jsonValue = Json.parse("{\"filter\":{\"page\":\"1\",\"localStartStations\":\"\",\"stopNames\":\"\",\"planTimes\":\"\",\"sort\":\"\",\"localYes\":\"\"},\"startCityId\":\"120628\",\"endCityId\":\"101329\",\"hasLines\":\"1\"}\t")
    val hasLines = (jsonValue \ "hasLines").as[String]

    log.info(jsonValue.toString())
    hasLines should be("1")
  }

  it should "get page \"1\"" in {
    val jsonValue = Json.parse("{\"filter\":{\"page\":\"1\",\"localStartStations\":\"\",\"stopNames\":\"\",\"planTimes\":\"\",\"sort\":\"\",\"localYes\":\"\"},\"startCityId\":\"120628\",\"endCityId\":\"101329\",\"page\":\"1\"}\t")
    val page = (jsonValue \ "filter" \ "page").as[String]

    log.info(jsonValue.toString())
    page should be("1")
  }
}
