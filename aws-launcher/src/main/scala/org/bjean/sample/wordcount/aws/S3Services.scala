package org.bjean.sample.wordcount.aws

import java.nio.file.{Path, Paths}
import java.time.Clock
import java.time.ZoneOffset._
import java.time.format.DateTimeFormatter

import com.amazonaws.services.s3.transfer.{Download, TransferManager, Upload}
import com.typesafe.config.Config
import org.bjean.sample.wordcount.aws.support.Retrying

import scala.concurrent.{ExecutionContext, Future}


object S3Services {
  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("YYYYMMddHHmm").withZone(UTC)
  val AWS_S3_UPLOAD_MAXATTEMPT: String = "aws.s3.upload.maxAttempt"
  val AWS_S3_BUCKET: String = "aws.s3.bucket"
  val AWS_S3_EXECUTION_DIR_PREFIX: String = "aws.s3.execution.dir.prefix"
}

class S3Services(transferManager: TransferManager, config: Config, clock: Clock)(implicit ec: ExecutionContext) extends Retrying {

  def copyExecutionContextFileToS3(executionContextFile: Path): Future[S3File] = {
    val maxAttempts: Int = config.getInt(S3Services.AWS_S3_UPLOAD_MAXATTEMPT)
    val s3file: S3File = getExecutionContextPath(executionContextFile.getFileName.toString)

    def uploadOperation = {
      val fileUpload = transferManager.upload(s3file.bucket, s3file.key, executionContextFile.toFile)
      fileUpload.waitForCompletion
      s3file
    }

    retry(uploadOperation, maxAttempts)
  }

  def getExecutionContextPath(executionContextFile: String): S3File = {
    val bucketName: String = config.getString(S3Services.AWS_S3_BUCKET)
    val executionContextS3Prefix: String = config.getString(S3Services.AWS_S3_EXECUTION_DIR_PREFIX)
    val executionTime: String = S3Services.formatter.format(clock.instant)
    val s3Key: String = Paths.get(executionContextS3Prefix, executionTime, executionContextFile).toString
    new S3NativeFile(bucketName, s3Key)
  }

  def copyResultToLocalPath[T <: S3File](processingResult: T, destinationFile: Path): Future[Download] = {
    Future {
      val fileDownload = transferManager.download(processingResult.bucket, processingResult.key, destinationFile.toFile)
      fileDownload.waitForCompletion
      fileDownload
    }
  }


}