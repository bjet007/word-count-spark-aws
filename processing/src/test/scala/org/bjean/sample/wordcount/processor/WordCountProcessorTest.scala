package org.bjean.sample.wordcount.processor

import java.nio.file.{Paths, Files}


import com.typesafe.config.Config
import org.apache.commons.lang.SystemUtils
import org.apache.spark.SparkContext
import org.bjean.sample.support.SparkContextTrait
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

import scala.io.Source

class WordCountProcessorTest extends WordSpec with Matchers with SparkContextTrait with BeforeAndAfter {

  var outputPath: String = _
  before {
    outputPath = Files.createTempDirectory("sm-processing-output").toAbsolutePath.toString
    Files.delete(Paths.get(outputPath))
    println(s"Output folder will be: ${outputPath}")
  }

  "Running WordCountProcessor" should {
    "output correct count for single word" in testForUseCase("single")
   }

  def inputFilePath(name: String) = getClass.getResource(s"/input/${name}.txt").getPath


  def testForUseCase(usecase: String, withRawSessionPathArgs: Boolean = true) = withSparkContext("a_spark_ctx") { (ctx: SparkContext) =>
    val expectedOutput = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(s"expected_output/${usecase}.txt")).toList

    val args = Map("input" -> inputFilePath(usecase), "output" -> outputPath)
   
    val job = new WordCountProcessor(ctx, args)
    job.runJob()
    val output = Source.fromFile(s"${outputPath}/part-00000").toList
    output should contain theSameElementsAs (expectedOutput)
  }

}
