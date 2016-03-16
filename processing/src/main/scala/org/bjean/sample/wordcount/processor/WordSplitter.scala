package org.bjean.sample.wordcount.processor

import scala.annotation.tailrec

object WordSplitter {
  def splitWord(word: String, wordLength: Int): List[String] = splitWordRec(word, wordLength, Set())
  

  @tailrec
  def splitWordRec(word: String, wordLength: Int, splitted: Set[String]): List[String] = word match {
    case x if (x.length < wordLength) => splitted.toList
    case x => {
      val words = x.grouped(wordLength).filter(subWord => subWord.length == wordLength)
      splitWordRec(x.substring(1), wordLength, splitted ++ words)
    }
  }
}
