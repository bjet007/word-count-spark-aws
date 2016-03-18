package org.bjean.sample.wordcount.processor

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class WordCountProcessor(val sparkContext: SparkContext, val config:Config) {
  def runJob(): Unit = {
    val textFile = sparkContext.textFile(config.input)
    val wordLength = config.wordLength

    val words: RDD[String] = textFile.flatMap(line => line.split(" "))
      .flatMap(word => WordSplitter.splitWord(word, wordLength))
    val counts = words
      .map(subWord => (subWord, 1))
      .reduceByKey(_ + _)

    counts.map(tuple => s"${tuple._1}\t${tuple._2}").coalesce(1).saveAsTextFile(config.output)
  }


}
