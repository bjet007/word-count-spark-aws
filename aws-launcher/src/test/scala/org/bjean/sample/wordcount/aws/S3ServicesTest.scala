package org.bjean.sample.wordcount.aws

import java.io.File
import java.nio.file.{Files, Paths}
import java.time.ZoneOffset.UTC
import java.time.{Clock, LocalDateTime}

import com.amazonaws.AmazonClientException
import com.amazonaws.services.s3.transfer.{Download, TransferManager, Upload}
import com.typesafe.config.Config
import org.bjean.sample.support.TemporaryFolder
import org.mockito.Matchers
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar


class S3ServicesTest extends FlatSpec with MockitoSugar with TemporaryFolder {

  trait TestSets {
    val clock: Clock = Clock.fixed(LocalDateTime.of(2015, 3, 22, 14, 56, 13).atZone(UTC).toInstant, UTC)
    val config = mock[Config]
    val fileUpload = mock[Upload]
    val fileDownload = mock[Download]

    val transferManager = mock[TransferManager]
    val sparkS3Services = new S3Services(transferManager, config, clock)

    when(config.getInt("aws.fs.s3.upload.maxattempt")).thenReturn(2)
    when(config.getString("aws.fs.s3.bucket")).thenReturn("my-bucket")
    when(config.getString("aws.fs.s3.execution.dir.prefix")).thenReturn("spark/execution/")

  }

  "A S3Service" should "upload a file into an execution context " in {
    new TestSets {
      when(transferManager.upload("my-bucket", "spark/execution/201503221456/myfile", new File("myfile"))).thenReturn(fileUpload)

      val s3File = sparkS3Services.copyExecutionContextFileToS3(Paths.get("myfile"))

      s3File should be(S3NativeFile("my-bucket", "spark/execution/201503221456/myfile"))
      verify(fileUpload).waitForCompletion()
      verify(transferManager).upload(any[String], any[String], any[File])
    }
  }

  it should " use a configurable s3 bucket" in {
    new TestSets {
      when(transferManager.upload(Matchers.eq("my-bucket"), any[String], any[File])).thenReturn(fileUpload)

      sparkS3Services.copyExecutionContextFileToS3(Paths.get("myfile"))

      verify(transferManager).upload(Matchers.eq("my-bucket"), any[String], any[File])
      verify(config).getString("aws.fs.s3.bucket")
    }
  }

  it should " use a configurable directory prefix" in {
    new TestSets {
      when(transferManager.upload(any[String], Matchers.eq("spark/execution/201503221456/myfile"), any[File])).thenReturn(fileUpload)

      sparkS3Services.copyExecutionContextFileToS3(Paths.get("myfile"))

      verify(transferManager).upload(any[String], Matchers.eq("spark/execution/201503221456/myfile"), any[File])
      verify(config).getString("aws.fs.s3.execution.dir.prefix")

    }
  }

  it should " retry the upload " in {
    new TestSets {
      when(transferManager.upload("my-bucket", "spark/execution/201503221456/myfile", new File("myfile"))).
        thenThrow(new AmazonClientException("")).
        thenReturn(fileUpload)

      val s3File = sparkS3Services.copyExecutionContextFileToS3(Paths.get("myfile"))
      
      s3File should be(S3NativeFile("my-bucket", "spark/execution/201503221456/myfile"))
      verify(fileUpload).waitForCompletion()
      verify(transferManager, times(2)).upload(any[String], any[String], any[File])
    }

  }

  it should " retry the upload until it reach the configured max attempt" in {
    new TestSets {
      when(transferManager.upload("my-bucket", "spark/execution/201503221456/myfile", new File("myfile"))).
        thenThrow(new AmazonClientException("")).
        thenThrow(new AmazonClientException(""))
      
      intercept[AmazonClientException] {
        sparkS3Services.copyExecutionContextFileToS3(Paths.get("myfile"))
      }
      
      verifyNoMoreInteractions(fileUpload)
      verify(transferManager, times(2)).upload(any[String], any[String], any[File])
      verify(config).getInt("aws.fs.s3.upload.maxattempt")
    }

  }

  it should " download a file to a local path" in {
    new TestSets {
      when(transferManager.download("my-bucket", "spark/execution/201503221456/myfile/part-00000", new File("myfile"))).thenReturn(fileDownload)
     
      sparkS3Services.copyResultToLocalPath(S3NativeFile("my-bucket", "spark/execution/201503221456/myfile/part-00000"), Paths.get("myfile"))
     
      verify(fileDownload).waitForCompletion()
      verify(transferManager).download(any[String], any[String], any[File])
    }

  }

  it should " download files to a local path" in {
    new TestSets {
      when(transferManager.download("my-bucket", "spark/execution/201503221456/myfile/part-00000", new File("myfile"))).thenReturn(fileDownload)
      when(transferManager.download("my-bucket", "spark/execution/201503221456/myfile2/part-00000", new File("myfile2"))).thenReturn(fileDownload)

      val filesToCopy = Map("key" -> (new S3NativeFile("my-bucket", "spark/execution/201503221456/myfile/part-00000"), Paths.get("myfile")),
        "key2" -> (new S3NativeFile("my-bucket", "spark/execution/201503221456/myfile2/part-00000"), Paths.get("myfile2")))

      val result = sparkS3Services.copyResultsToLocalPath(filesToCopy, testFolder.toPath)
      verify(fileDownload, times(2)).waitForCompletion()
      verify(transferManager, times(2)).download(any[String], any[String], any[File])

      val successFile = testFolder.toPath.resolve("downloads.success")
      Files.exists(successFile) should be(true)
      result("key") should be(Paths.get("myfile"))
      result("key2") should be(Paths.get("myfile2"))
      result should contain key "downloads.success"
    }
  }
}
