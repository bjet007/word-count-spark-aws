package org.bjean.sample.support

import java.io.File

import org.scalatest.{Outcome, SuiteMixin, Suite}

trait TemporaryFolder extends SuiteMixin {
  this: Suite =>
  var testFolder: File = _

  private def deleteFile(file: File) {
    if (!file.exists) return
    if (file.isFile) {
      file.delete()
    } else {
      file.listFiles().foreach(deleteFile)
      file.delete()
    }
  }

  abstract override def withFixture(test: NoArgTest): Outcome = {
    val tempFolder = System.getProperty("java.io.tmpdir")
    var folder: File = null

    do {
      folder = new File(tempFolder, "scalatest-" + System.nanoTime)
    } while (!folder.mkdir())

    testFolder = folder

    try {
      super.withFixture(test)
    } finally {
      deleteFile(testFolder)
    }
  }
}