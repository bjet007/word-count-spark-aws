package org.bjean.sample.wordcount.aws.support

import scala.concurrent.{ExecutionContext, Future}

trait Retrying {
  def retry[T](op: => T, retries: Int)(implicit ec: ExecutionContext): Future[T] = {
    val eventualT: Future[T] = Future(op)
    eventualT recoverWith {
      case _ if retries > 0 => retry(op, retries - 1)
    }
  }
}
