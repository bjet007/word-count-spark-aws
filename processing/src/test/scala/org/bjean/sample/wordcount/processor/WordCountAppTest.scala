package org.bjean.sample.wordcount.processor

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.file.{Files, Paths}

import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}


class WordCountAppTest extends FlatSpec with Matchers  {

  "A Word Count App" should " return usage when input is not defined" in {
    val baos = new ByteArrayOutputStream
    val ps = new PrintStream(baos)
    Console.withOut(ps)(WordCountApp.main(Array("--output", "myout")))

    baos.toString shouldBe theUsage
  }

  it should " return usage when output is not defined" in {
    val baos = new ByteArrayOutputStream
    val ps = new PrintStream(baos)
    Console.withOut(ps)(WordCountApp.main(Array("--input", "myin")))

    baos.toString shouldBe theUsage
  }

  private def theUsage: String = {
    """Enhanced Word Count 1.0
      |Usage: spark-submit --deploy-mode cluster 
      |             --master yarn-cluster
      |             --class org.bjean.sample.wordcount.processor.WordCountApp 
      |             pathTo/processing-assembly-0.1-SNAPSHOT.jar
      |             --input hdfs://somepath/
      |             --output hdfs://outpath/,
      |             --wordLength 4 [options]
      |
      |  -i <URI> | --input <URI>
      |        Data Input. Comma separated spark path ie:./somePath,htfs://somepath/,s3://somepath
      |  -o <URI> | --output <URI>
      |        Spark output path ie: s3://someDestinationPath
      |  -w <number> | --wordLength <number>
      |        Number of char withing a work to create a pattern. Default 4
      |To get Better Result run in a cluster spark
      |
      |  --help
      |        Prints this usage text
      |""".stripMargin
  }

  private def inputFilePath(name: String) = getClass.getResource(s"/input/${name}.txt").getPath
  private def outputFilePath(name: String) = getClass.getResource(s"/output/${name}.txt").getPath

}
