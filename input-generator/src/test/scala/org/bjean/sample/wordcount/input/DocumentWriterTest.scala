package org.bjean.sample.wordcount.input

import java.nio.file.{Files, Path}

import org.bjean.sample.support.TemporaryFolder
import org.scalatest.{Matchers, FlatSpec}

class DocumentWriterTest extends FlatSpec with Matchers with TemporaryFolder{

  val documentWriter:DocumentWriter = new DocumentWriter()
  "A Document writer" should "write data to a file" in {

    val path: Path = testFolder.toPath().resolve("data")
    documentWriter.write(path, "any string")
   
    Files.readAllLines(path) should contain only ("any string")
  }
}
