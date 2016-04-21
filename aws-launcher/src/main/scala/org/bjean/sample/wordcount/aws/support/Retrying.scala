package org.bjean.sample.wordcount.aws.support

import akka.actor.Scheduler

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import akka.pattern.after

trait Retrying {
  def retry[T](op: => T, retries: Int)(implicit ec: ExecutionContext): Future[T] = {
    val eventualT: Future[T] = Future(op)
    eventualT recoverWith {
      case _ if retries > 0 => retry(op, retries - 1)
    }
  }

  def retry[T](eventualT: => Future[T], delay: FiniteDuration, retries: Int)(implicit ec: ExecutionContext, s: Scheduler): Future[T] = {
    eventualT recoverWith {
      case _ if retries > 0 => after(delay, s)(retry(eventualT, delay, retries - 1))
    }
  }
}
