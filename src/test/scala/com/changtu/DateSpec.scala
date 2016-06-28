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

}
