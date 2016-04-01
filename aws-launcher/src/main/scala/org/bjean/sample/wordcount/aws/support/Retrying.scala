package org.bjean.sample.wordcount.aws.support

import akka.actor.Scheduler
import akka.pattern.after

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

trait Retrying {
  def retry[T](op: => T, delay: FiniteDuration, retries: Int)(implicit ec: ExecutionContext, s: Scheduler): Future[T] =
    Future(op) recoverWith { case _ if retries > 0 => after(delay, s)(retry(op, delay, retries - 1)) }
}
