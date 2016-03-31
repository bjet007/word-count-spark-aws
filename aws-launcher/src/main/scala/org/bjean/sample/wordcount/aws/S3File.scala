package org.bjean.sample.wordcount.aws

import java.lang.String.format

object S3File {
  val s3NativePattern = "s3n://(.*)/(.*)".r
  val s3BlockPattern = "s3://(.*)/(.*)".r

  def apply(s3FileName: String): S3File = s3FileName match {
    case s3NativePattern(bucket, key) => S3NativeFile(bucket, key)
    case s3BlockPattern(bucket, key) => S3BlockFile(bucket, key)

  }
}

trait S3File {
  val bucket: String
  val key: String
  val s3Prefix: String

  def toS3Path: String = format("%s://%s/%s", s3Prefix, bucket, key)

}

case class S3NativeFile(bucket: String, key: String) extends S3File {
  val s3Prefix = "s3n"
}

case class S3BlockFile(bucket: String, key: String) extends S3File {
  val s3Prefix = "s3"
}