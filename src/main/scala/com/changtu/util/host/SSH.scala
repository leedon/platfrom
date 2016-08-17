package com.changtu.util.host

import com.jcraft.jsch.{ChannelExec, JSch}

/**
  * Created by lubinsu on 8/11/2016.
  */
object SSH {

  def apply(remoteMachine: String, userName: String, port: Int = 22, command: String, keyfile: String, password: String): Int = {

    val jsch = new JSch()
    //jsch.addIdentity(keyfile)

    val session = jsch.getSession(userName, remoteMachine, port)
    //设置登陆主机的密码
    //设置密码
    session.setPassword(password)
    //设置第一次登陆的时候提示，可选值：(ask | yes | no)
    session.setConfig("StrictHostKeyChecking", "no")
    //设置登陆超时时间
    session.connect(30000)

    val openChannel = session.openChannel("exec").asInstanceOf[ChannelExec]
    openChannel.setCommand(command)
    openChannel.connect()
    openChannel.disconnect()
    session.disconnect()
    0
  }
}
