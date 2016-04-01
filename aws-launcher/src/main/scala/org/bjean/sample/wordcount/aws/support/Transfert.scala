package org.bjean.sample.wordcount.aws.support

import com.amazonaws.event.ProgressEventType.{TRANSFER_CANCELED_EVENT, TRANSFER_COMPLETED_EVENT, TRANSFER_FAILED_EVENT}
import com.amazonaws.event.{ProgressEvent, ProgressListener}
import com.amazonaws.services.s3.transfer.Transfer
import com.amazonaws.services.s3.transfer.Transfer.TransferState
import com.amazonaws.services.s3.transfer.Transfer.TransferState.{Canceled, Completed, Failed}

import scala.concurrent.{Future, Promise}

/**
  * Inpired by https://github.com/dwhjames/aws-wrap
  */
object Transfert {

  import com.amazonaws.services.s3.transfer.internal.{AbstractTransfer, TransferStateChangeListener}

  def listenFor[T <: Transfer](transfer: T): Future[transfer.type] = {

    val p = Promise[transfer.type]

    if (transfer.isInstanceOf[AbstractTransfer]) {
      transfer.asInstanceOf[AbstractTransfer].addStateChangeListener(aStateListener(transfer)(p))
    }
    transfer.addProgressListener(aProgressListener(transfer)(p))

    if (transfer.isDone) {
      val success = p trySuccess transfer
    }

    p.future
  }

  def aProgressListener[T <: Transfer](transfer: T)(p: Promise[transfer.type]): ProgressListener = {
    new ProgressListener {
      override def progressChanged(progressEvent: ProgressEvent): Unit = {
        progressEvent.getEventType() match {
          case TRANSFER_CANCELED_EVENT | TRANSFER_COMPLETED_EVENT | TRANSFER_FAILED_EVENT => p trySuccess transfer
          case _ =>   
        }
      }
    }
  }

  def aStateListener[T <: Transfer](transfer: T)(p: Promise[transfer.type]): TransferStateChangeListener = {
    new TransferStateChangeListener {
      override def transferStateChanged(t: Transfer, state: TransferState): Unit = state match {
        case Completed | Canceled | Failed => p trySuccess transfer
        case _ =>
      }
    }
  }
}
