package org.bjean.sample.wordcount.input

import java.nio.file.{Files, Path}

import org.bjean.sample.support.TemporaryFolder
import org.scalatest.{Matchers, FlatSpec}

class DocumentWriterTest extends FlatSpec with Matchers with TemporaryFolder{

  val documentWriter:DocumentWriter = new DocumentWriter(25)
  "A Document writer" should "write data to a file" in {

    val path: Path = testFolder.toPath().resolve("data")
    documentWriter.write(path, "any string")
   
    Files.readAllLines(path) should contain only ("any string")
  }
  
  it should "wrap line to a maximum of characters" in {

    val path: Path = testFolder.toPath().resolve("data")
    documentWriter.write(path, "any string on first line second lines")

    Files.readAllLines(path) should contain only ("any string on first line","second lines" )

  }

  it should "not wrap long word" in {

    val path: Path = testFolder.toPath().resolve("data")
    documentWriter.write(path, "any string on first verylongword second lines")

    Files.readAllLines(path) should contain only ("any string on first","verylongword second lines" )

  }
}
