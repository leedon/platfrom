package com.changtu

import com.changtu.util.hbase.HBaseClient
import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by lubinsu on 2016/6/15.
  * HBase工具类单元测试
  */
class HBaseSpec extends FlatSpec with Matchers {

  "HBase" should "get user label 1251212,112312,325123" in {

    val hbaseClient = new HBaseClient(tablePath = "bi_user_label_string")

    val get = hbaseClient.getGet("1735909")
    val value = hbaseClient.get(get).getValue(Bytes.toBytes("labels"), Bytes.toBytes(""))
    println(Bytes.toString(value))
    Bytes.toString(value) should be("1251212,112312,325123")
  }

  it should "get all label details of 872547" in {

    val hbaseClient = new HBaseClient(tablePath = "bi_user_label" /*, verboseMode = true*/)

    hbaseClient.scanPrefix("872547_", p => println(Bytes.toString(p.getValue(Bytes.toBytes("label_info"), Bytes.toBytes("label_code")))))
  }
}
