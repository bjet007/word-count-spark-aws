package org.bjean.sample.wordcount.aws.support

import org.scalatest.Assertions
import org.scalatest.concurrent.AsyncAssertions

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object ScalaTestFutureHelper {
    implicit class Failing[A](val f: Future[A])(implicit ec: ExecutionContext) extends Assertions with AsyncAssertions {
      def failing[T <: Throwable](implicit m: Manifest[T]) = {
        val w = new Waiter
        f onComplete {
          case Failure(e) => w(throw e); w.dismiss()
          case Success(_) => w.dismiss()
        }
        intercept[T] {
          w.await
        }
      }
    }
}
