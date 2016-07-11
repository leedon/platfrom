package com.changtu

import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by 6526 on 6/27/2016.
  */
class DateSpec extends FlatSpec with Matchers {

  "Time" should "return bigger" in {

    DateTime.now().plusDays(-21).getMillis < DateTime.now().plusDays(-18).getMillis should be(true)

  }

  it should "return the length" in {
    val str = "visit:,ogUrl=http://www.trip8080.com/chezhan/xuchangshi/xuchangshi13126.html:logHisRefer=-:title=禹州汽"
    println((str.getBytes("GBK").length - 100).toDouble / 2)
    println(str.substring(0, 100 - ((str.getBytes("GBK").length - 100).toDouble /2).round.toInt))
    println(str.getBytes("UTF-8").length)
    println(str.getBytes("GBK").length)
    println(str.subSequence(0, 100))
    //str.length should be(100)
  }

}
