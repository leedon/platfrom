package com.changtu

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import com.changtu.util.Logging
import org.scalatest.{FlatSpec, Matchers}

/**
  * 测试示例
  */
class ExampleSpec extends FlatSpec with Matchers with Logging {

  "A Stack" should "pop values in last-in-first-out order" in {
    val loc = new Locale("en")
    val fm = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz y", loc)
    val tm = "Fri Jul 27 12:38:20 CST 2016"
    val dt2 = fm.parse(tm)
    println(dt2.toString)
  }

}
