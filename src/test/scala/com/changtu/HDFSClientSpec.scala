package com.changtu

import com.changtu.hdfs.HDFSClient
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by lubinsu on 2016/6/11.
  * 测试 HDFS client工具类
  */
class HDFSClientSpec extends FlatSpec with Matchers {

  "HDFS client" should "create a file" in {
    HDFSClient.createDirectory("/user/hadoop/biglog/test", deleteF = true) should be(true)
    HDFSClient.release()
  }
}
