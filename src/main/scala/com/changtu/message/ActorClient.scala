package com.changtu.message

import akka.actor.{ActorSystem, Props}

/**
  * Created by lubinsu on 2016/3/29
  */
object ActorClient {

  case class Ip(ipAddr: String)

  def main(args: Array[String]) {
    val system = ActorSystem("MySystem")
    /*val myActor = system.actorOf(Props[MyActor], name = "myactor")

    myActor ! Ip("221.6.35.202")
    myActor ! Ip("121.42.156.132")
    Thread.sleep(1000)
    println(myActor.isTerminated)*/
    //system.shutdown()
  }
}