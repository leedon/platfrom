package com.changtu.pub

import scala.io.Source

/**
  * Created by lubinsu on 5/5/2016
  */
object JdbcProperty extends Serializable{
  private var props: Map[String, String] = Map()

  /**
    * 加载目标目录下的jdbc配置文件
    * @param filePath 路径
    */
  def load(filePath: String): Map[String, String] = {
    var path = filePath
    if (filePath.length == 0 ) {
      path = "/appl/conf"
    }
    Source.fromFile(path + "/jdbc.properties").getLines().filterNot(_.startsWith("#")).map(_.split("=")).foreach(setAsProps)
    props
  }

  /**
    * 加载
    * @param key key键
    * @param value value值
    */
  def set(key: String, value: String): Unit = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key)
    }
    props += (key -> value)
  }

  /**
    * 将每一行键值对set到Map中
    */
  def setAsProps[U](arr: Array[String]): Unit = {
    arr.length match {
      case 2 => set(arr(0), arr(1))
      case _ =>
    }
  }

  /**
    *
    * @param key 属性文件的KEY
    * @return
    */
  def getOption(key: String): Option[String] = {
    props.get(key)
  }

  /**
    *
    * @param key 属性文件的KEY
    * @return
    */
  def get(key: String): String = {
    getOption(key).getOrElse(throw new NoSuchElementException(key))
  }
}
