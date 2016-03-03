package org.bjean.sample.wordcount.input

import org.scalatest.{FlatSpec, Matchers}

class DocumentGeneratorTest extends FlatSpec with Matchers {

  val documentGenerator = new DocumentGenerator()
  
  "A Document Generator" should  "generate random word" in {
    val score = documentGenerator.nextWord()
    score should not be (null)
    score shouldBe a [String]
  }
}
