package org.bjean.sample.wordcount.aws

import java.io.IOException
import java.nio.file.{Path, Paths}
import java.time.Clock
import java.time.ZoneOffset._
import java.time.format.DateTimeFormatter

import com.amazonaws.services.s3.transfer.{Download, TransferManager, Upload}
import com.typesafe.config.Config
import org.apache.commons.io.FileUtils


object S3Services {
  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("YYYYMMddHHmm").withZone(UTC)
  val AWS_S3_UPLOAD_MAXATTEMPT: String = "aws.fs.s3.upload.maxattempt"
  val AWS_S3_BUCKET: String = "aws.fs.s3.bucket"
  val AWS_S3_EXECUTION_DIR_PREFIX: String = "aws.fs.s3.execution.dir.prefix"
  val DOWNLOADS_SUCCESS_FILE: String = "downloads.success"
}

class S3Services(transferManager: TransferManager, config: Config, clock: Clock) {

  @throws(classOf[InterruptedException])
  def copyExecutionContextFileToS3(executionContextFile: Path): S3File = {
    val maxAttempts: Int = config.getInt(S3Services.AWS_S3_UPLOAD_MAXATTEMPT)
    val s3file: S3File = getExecutionContextPath(executionContextFile.getFileName.toString)
    var attempts: Int = 1
    retry(maxAttempts) {
      val fileUpload: Upload = transferManager.upload(s3file.bucket, s3file.key, executionContextFile.toFile)
      fileUpload.waitForCompletion
      s3file
    }
  }

  def getExecutionContextPath(executionContextFile: String): S3File = {
    val bucketName: String = config.getString(S3Services.AWS_S3_BUCKET)
    val executionContextS3Prefix: String = config.getString(S3Services.AWS_S3_EXECUTION_DIR_PREFIX)
    val executionTime: String = S3Services.formatter.format(clock.instant)
    val s3Key: String = Paths.get(executionContextS3Prefix, executionTime, executionContextFile).toString
    new S3NativeFile(bucketName, s3Key)
  }

  @throws(classOf[InterruptedException])
  def copyResultToLocalPath[T <: S3File](processingResult: T, destinationFile: Path) {
    val fileDownload: Download = transferManager.download(processingResult.bucket, processingResult.key, destinationFile.toFile)
    fileDownload.waitForCompletion
  }

  @throws(classOf[InterruptedException])
  @throws(classOf[IOException])
  def copyResultsToLocalPath[T <: S3File](filesToDownload: Map[String, (T, Path)], successFileOutputPath: Path): Map[String, Path] = {

    val filesDestination: Map[String, Path] =
      for (file <- filesToDownload) yield {
        val fileToDownload: T = file._2._1
        val destinationPath: Path = file._2._2
        copyResultToLocalPath(fileToDownload, destinationPath)
        file._1 -> destinationPath
      }
    if (filesDestination.size != filesToDownload.size) {
      return filesDestination
    }
    val successFilePath: Path = successFileOutputPath.resolve(S3Services.DOWNLOADS_SUCCESS_FILE)
    FileUtils.touch(successFilePath.toFile)
    filesDestination + (S3Services.DOWNLOADS_SUCCESS_FILE -> successFilePath)
  }

  @annotation.tailrec
  private def retry[T](n: Int)(fn: => T): T = {
    util.Try {
      fn
    } match {
      case util.Success(x) => x
      case _ if n > 1 => retry(n - 1)(fn)
      case util.Failure(e) => throw e
    }
  }


}