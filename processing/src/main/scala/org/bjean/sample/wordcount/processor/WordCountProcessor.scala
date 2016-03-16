package org.bjean.sample.wordcount.processor

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class WordCountProcessor(val sparkContext: SparkContext, val args: Map[String, String]) {
  def runJob(): Unit = {
    val textFile = sparkContext.textFile(args.get("input").get)
    val wordLength = args.get("wordLength").map(_.toInt).get

    val words: RDD[String] = textFile.flatMap(line => line.split(" "))
      .flatMap(word => WordSplitter.splitWord(word, wordLength))
    val counts = words
      .map(subWord => (subWord, 1))
      .reduceByKey(_ + _)

    counts.map(tuple => s"${tuple._1}\t${tuple._2}").coalesce(1).saveAsTextFile(args.get("output").get)
  }


}
