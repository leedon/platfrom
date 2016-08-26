package com.changtu.service

import akka.actor.{Actor, Props}
import com.changtu.core.Host
import com.changtu.util.Logging

/**
  * Created by lubinsu on 8/23/2016.
  * 命令派发
  */
class CollectMonitor extends Actor with Logging{
  override def receive: Receive = {
    case Host(host, port, cmd) =>
      val collector = context.actorOf(Props[CollectLogService], "collector-".concat(host))
      collector ! Host(host, port, cmd)
    case _ => logger.warn("unknown operation.")
  }
}
