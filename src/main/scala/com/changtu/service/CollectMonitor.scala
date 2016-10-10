package com.changtu.service

import akka.actor.{Actor, Props}
import com.changtu.core.Host
import com.changtu.util.Logging
import com.changtu.util.host.{AES, Configuration, SSH}

import scala.util.{Failure, Success}

/**
  * Created by lubinsu on 8/23/2016.
  * 命令派发
  */
class CollectMonitor extends Actor with Logging {
  override def receive: Receive = {
    case Host(host, port, cmd) =>
      getLogFiles(host, port, cmd)
      val collector = context.actorOf(Props[CollectLogService], "collector-".concat(host))
      context.children.foreach( p => {
        println(p.path.name)
      })
      collector ! Host(host, port, cmd)
    case _ => logger.warn("unknown operation.")
  }

  private def getLogFiles(host: String, port: String, cmd: String): Int = {
    // 密码解密
    val password = AES.decrypt(Configuration("passwd").getString(host.concat("-hadoop")), "secretKey.changtu.com") match {
      case Success(encrypted) =>
        encrypted.asInstanceOf[String]
      case Failure(e) =>
        logger.error(e.getMessage)
        ""
    }

    val ssh = (cmd: String) => SSH(host, "hadoop", port.toInt, cmd, "", password)
    ssh("find /appl/logs -type f")
  }

}
