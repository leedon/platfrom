package com.changtu.message

/**
  * Created by lubinsu on 2016/3/29
  */
object ProdConSample {

  class Producer(drop: Drop) extends Runnable {
    val importantInfo: Array[String] = Array(
      "Mares eat oats",
      "Does eat oats",
      "Little lambs eat ivy",
      "A kid will eat ivy too")

    override def run(): Unit = {
      importantInfo.foreach((msg) => drop.put(msg))
      drop.put("DONE")
    }
  }

  class Consumer(drop: Drop) extends Runnable {
    override def run(): Unit = {
      var message = drop.take()
      while (message != "DONE") {
        System.out.format("MESSAGE RECEIVED: %s%n", message)
        message = drop.take()
      }
    }
  }

  class Drop {
    var message: String = ""
    var empty: Boolean = true
    var lock: AnyRef = new Object()

    def put(x: String): Unit =
      lock.synchronized {
        // Wait until message has been retrieved
        await(empty == true)
        // Toggle status
        empty = false
        // Store message
        message = x
        // Notify consumer that status has changed
        lock.notifyAll()
      }

    def take(): String =
      lock.synchronized {
        // Wait until message is available.
        await(empty == false)
        // Toggle status
        empty = true
        // Notify producer that staus has changed
        lock.notifyAll()
        // Return the message
        message
      }

    private def await(cond: => Boolean) =
      while (!cond) {
        lock.wait()
      }

  }

  def main(args: Array[String]): Unit = {
    // Create Drop
    val drop = new Drop()

    // Spawn Producer
    new Thread(new Producer(drop)).start()

    // Spawn Consumer
    new Thread(new Consumer(drop)).start()
  }
}
