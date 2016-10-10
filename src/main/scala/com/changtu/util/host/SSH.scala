package com.changtu.util.host

import java.io.{BufferedReader, InputStreamReader}

import com.changtu.util.Logging
import com.jcraft.jsch.{ChannelExec, JSch}

/**
  * Created by lubinsu on 8/11/2016.
  */
object SSH extends Logging {

  def apply(remoteMachine: String, userName: String, port: Int = 22,
            command: String, keyfile: String, password: String,
            outputFum: (String, String) => Unit = { (msg, host) =>
              logger.info(msg)
            }): Int = {

    val jsch = new JSch()
    //jsch.addIdentity(keyfile)

    val session = jsch.getSession(userName, remoteMachine, port)
    //设置登陆主机的密码
    //设置密码
    session.setPassword(password)
    //设置第一次登陆的时候提示，可选值：(ask | yes | no)
    session.setConfig("StrictHostKeyChecking", "no")
    //设置登陆超时时间
    session.connect(5000)

    val openChannel = session.openChannel("exec").asInstanceOf[ChannelExec]
    openChannel.setCommand(command)
    //openChannel.setOutputStream(output)
    openChannel.setPty(false)
    val input = openChannel.getInputStream
    openChannel.connect()
    val bufferReader = new BufferedReader(new InputStreamReader(input))

    while (!(openChannel.isClosed || input.available() < 0)) {
      while (bufferReader.ready()) {
        val msg = bufferReader.readLine
        //执行传入的函数处理返回信息
        outputFum(msg, remoteMachine)
      }
      Thread.sleep(500)
    }
    val exit = openChannel.getExitStatus
    logger.info(command.concat(",ssh command exit status: ").concat(exit.toString))
    bufferReader.close()
    openChannel.disconnect()
    session.disconnect()
    exit
  }

}
