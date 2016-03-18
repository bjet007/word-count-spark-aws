package org.bjean.sample.wordcount.processor

import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

object WordCountApp extends App {

  aCliParser().parse(args, Config()) match {
    case Some(config) => new WordCountProcessor(aSparkContext(), config)
    case None => println(aCliParser().usage)
  }

  def aSparkContext(): SparkContext = {
    val conf = new SparkConf().setAppName("Enhanced Word Count")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    new SparkContext(conf)
  }

  def aCliParser(): OptionParser[Config] = {
    new OptionParser[Config]("""spark-submit --deploy-mode cluster 
             --master yarn-cluster
             --class org.bjean.sample.wordcount.processor.WordCountApp 
             pathTo/processing-assembly-0.1-SNAPSHOT.jar
             --input hdfs://somepath/
             --output hdfs://outpath/,
             --wordLength 4""") {
      head("Enhanced Word Count", "1.0")
      opt[String]('i', "input") required() valueName "<URI>" action { (x, c) =>
        c.copy(input = x)
      } text "Data Input. Comma separated spark path ie:./somePath,htfs://somepath/,s3://somepath"
      opt[String]('o', "output") required() valueName "<URI>" action { (x, c) =>
        c.copy(output = x)
      } text "Spark output path ie: s3://someDestinationPath"
      opt[Int]('w', "wordLength") valueName "<number>" action { (x, c) =>
        c.copy(wordLength = x)
      } text "Number of char withing a work to create a pattern. Default 4"
      note("To get Better Result run in a cluster spark\n")
      help("help") text "Prints this usage text"

    }
  }
}

