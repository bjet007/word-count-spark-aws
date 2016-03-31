package org.bjean.sample.wordcount.aws

import org.scalatest.{FunSuite, Matchers}

class S3FileTest extends FunSuite with Matchers {

   
    test(" toS3Path"){
        S3BlockFile("bucket","key").toS3Path shouldBe "s3://bucket/key"
        S3NativeFile("bucket","key").toS3Path shouldBe "s3n://bucket/key"
    }

    test(" froS3Path"){
        S3File("s3://bucket/key" ) shouldBe  S3BlockFile("bucket","key")
        S3File("s3n://bucket/key")  shouldBe  S3NativeFile("bucket","key")
    }

}
