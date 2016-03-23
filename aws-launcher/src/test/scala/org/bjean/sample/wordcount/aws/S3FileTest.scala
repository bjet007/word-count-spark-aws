package org.bjean.sample.wordcount.aws

import aws.{S3File, S3Type}
import com.google.common.testing.EqualsTester
import junit.framework.AssertionFailedError
import org.scalatest.{FunSuite, Matchers}

class S3FileTest extends FunSuite with Matchers {

    test(" Equals and Hashcode"){
        new EqualsTester().addEqualityGroup(new S3File("bucket","key"), new S3File("bucket","key")).testEquals()
    }

    test(" Equals and Hashcode with Type"){
        new EqualsTester().addEqualityGroup(new S3File("bucket","key", S3Type.BLOCK), new S3File("bucket","key",S3Type.BLOCK)).testEquals()
    }

    test(" Not Equals"){
        intercept[AssertionFailedError] {
            new EqualsTester().addEqualityGroup(new S3File("bucket", "key"), new S3File("bucket2", "key")).testEquals()
        }
        intercept[AssertionFailedError] {
            new EqualsTester().addEqualityGroup(new S3File("bucket", "key"), new S3File("bucket", "key2")).testEquals()
        }

        intercept[AssertionFailedError] {
            new EqualsTester().addEqualityGroup(new S3File("bucket", "key", S3Type.BLOCK), new S3File("bucket", "key",S3Type.NATIVE)).testEquals()
        }
    }


    test(" toS3Path"){
        new S3File("bucket","key").toS3Path() shouldBe "s3://bucket/key"
        new S3File("bucket","key",S3Type.NATIVE).toS3Path shouldBe "s3n://bucket/key"
        new S3File("bucket","key",S3Type.BLOCK).toS3Path shouldBe "s3://bucket/key"
    }

    test(" froS3Path"){
        S3File.fromS3Path("s3://bucket/key" ) shouldBe new S3File("bucket","key")
        S3File.fromS3Path("s3n://bucket/key")  shouldBe new S3File("bucket","key",S3Type.NATIVE)
        S3File.fromS3Path("s3://bucket/key")  shouldBe new S3File("bucket","key",S3Type.BLOCK)
    }

}
