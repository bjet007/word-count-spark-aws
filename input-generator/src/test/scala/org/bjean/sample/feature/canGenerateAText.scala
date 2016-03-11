package org.bjean.sample.feature

import java.nio.file.{Files, Path}

import org.bjean.sample.support.TemporaryFolder
import org.bjean.sample.wordcount.input.{DocumentGenerator, DocumentWriter}
import org.scalatest.{FlatSpec, Matchers}


class canGenerateAText extends FlatSpec with Matchers with TemporaryFolder{

  "A Document Generator" should "write a random text to a file" in {

    val path: Path = testFolder.toPath().resolve("data")
    DocumentGenerator.main(s"${path.toFile.getAbsolutePath}")
    val text: String = scala.io.Source.fromFile(path.toFile).mkString
    text shouldBe a[String]
    "\t".r.findAllIn(text).length shouldBe 2
  }

  "A Document Generate with -n 3" should "write a random text with 3 paragraph" in {

    val path: Path = testFolder.toPath().resolve("data")
    DocumentGenerator.main("-n","3",s"${path.toFile.getAbsolutePath}")
    val text: String = scala.io.Source.fromFile(path.toFile).mkString
    text shouldBe a[String]
    "\t".r.findAllIn(text).length shouldBe 3
  }
}
