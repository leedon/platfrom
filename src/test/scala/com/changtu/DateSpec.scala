package com.changtu

import java.text.SimpleDateFormat

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by 6526 on 6/27/2016.
  */
class DateSpec extends FlatSpec with Matchers {

  "Time" should "return bigger" in {

    DateTime.now().plusDays(-21).getMillis < DateTime.now().plusDays(-18).getMillis should be(true)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS")
    val format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss:SSS")
    println(sdf.parse("12016-08-12 15:47:29:230"))
    println("2016-08-12 15:01:29:230".length)
  }

  it should "return the length" in {
    val str = "visit:,ogUrl=http://www.trip8080.com/chezhan/xuchangshi/xuchangshi13126.html:logHisRefer=-:title=禹州汽"
    println((str.getBytes("GBK").length - 100).toDouble / 2)
    println(str.substring(0, 100 - ((str.getBytes("GBK").length - 100).toDouble / 2).round.toInt))
    println(str.getBytes("UTF-8").length)
    println(str.getBytes("GBK").length)
    println(str.subSequence(0, 100))
    //str.length should be(100)
  }

}
