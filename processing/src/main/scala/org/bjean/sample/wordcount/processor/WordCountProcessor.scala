package org.bjean.sample.wordcount.processor

import org.apache.spark.SparkContext

class WordCountProcessor(val sparkContext: SparkContext, val args: Map[String, String] ){
  def runJob():Unit = {
   val textFile = sparkContext.textFile(args.get("input").get)

    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile(args.get("output").get)
  }

}
