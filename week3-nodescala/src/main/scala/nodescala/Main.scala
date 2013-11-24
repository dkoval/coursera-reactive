package nodescala

import scala.language.postfixOps
import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global
import scala.util.{Success, Failure}

object Main {

  def main(args: Array[String]) {
    // TO IMPLEMENT
    // 1. instantiate the server at 8191, relative path "/test",
    //    and have the response return headers of the request
    val myServer = new NodeScala.Default(8191)
    val myServerSubscription = myServer.start("/test") {
      req => "Have a nice day!".split(" ").iterator
    }

    // TO IMPLEMENT
    // 2. create a future that expects some user input `x`
    //    and continues with a `"You entered... " + x` message
    val userInterrupted: Future[String] = Future.userInput("").continue {
      case Success(x) => "You entered... " + x
      case Failure(e) => throw e
    }

    def timeOutIn(d: Duration): Future[String] = {
      val p = Promise[String]()
      Future.delay(d) onComplete { case _ => p.success("Server timeout!") }
      p.future
    }

    // TO IMPLEMENT
    // 3. create a future that completes after 20 seconds
    //    and continues with a `"Server timeout!"` message
    val timeOut: Future[String] = timeOutIn(20 seconds)


    // TO IMPLEMENT
    // 4. create a future that completes when either 10 seconds elapse
    //    or the user enters some text and presses ENTER
    val terminationRequested: Future[String] = {
      Future.any(List(userInterrupted, timeOut))
    }

    // TO IMPLEMENT
    // 5. unsubscribe from the server
    terminationRequested onSuccess {
      case msg =>
        println(msg)
        println("Unsubscribe from the server!")
        myServerSubscription.unsubscribe()
    }
  }

}