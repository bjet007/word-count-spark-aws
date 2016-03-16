package org.bjean.sample.wordcount.processor

import org.scalatest.{FlatSpec, Matchers}

class WordSplitterTest extends FlatSpec with Matchers {

  "A Word Splitter " should "split words" in {

    WordSplitter.splitWord("oneword", 4) should contain only("onew", "newo", "ewor", "word")
  }

  it should "split neworder correctly" in {
    val result = WordSplitter.splitWord("neworder", 4)
    result should have length 5
    result should contain only("newo", "ewor", "word", "orde", "rder")

  }
  
}
