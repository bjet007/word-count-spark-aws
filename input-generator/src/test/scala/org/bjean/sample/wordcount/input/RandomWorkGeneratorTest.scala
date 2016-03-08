package org.bjean.sample.wordcount.input

import java.util.Random

import org.scalatest.{Matchers, FlatSpec}

class RandomWorkGeneratorTest extends FlatSpec with Matchers {

  val wordGenerator = new RandomWordGenerator(new Random())

  "A Document Generator" should "generate random word" in {
    wordGenerator.nextWord() should not be (null)
  }

  it should "generate random word of type string" in {
    wordGenerator.nextWord() shouldBe a[String]
  }

  it should "generate random word matching the regexp ^[a-z]*$" in {
    wordGenerator.nextWord() should fullyMatch regex "^[a-z]*$"
  }

  it should "generate random word that have at leat 5 chars" in {
    wordGenerator.nextWord().length should be >= 5
  }

  it should "generate random sentence" in {
    wordGenerator.nextSentence() should not be (null)
  }

  it should "generate random sentence of type string" in {
    wordGenerator.nextSentence() shouldBe a[String]
  }

  it should "generate random sentence matching the regexp ^[a-z \\.]*$" in {
    wordGenerator.nextSentence() should fullyMatch regex "^[a-z \\.]*$"
  }

  it should "generate random sentence that have at least 2 words" in {
    wordGenerator.nextSentence().length should be >= 13
  }

  it should "generate random sentence that end with a ." in {
    wordGenerator.nextSentence() should endWith(".")
  }

  it should "generate random paragraph that start with a tab" in {
    wordGenerator.nextParagraph() should startWith("\t")
  }

  it should "generate random paragraph should contains at leat 2 sentences" in {
    val paragraph: String = wordGenerator.nextParagraph()
    paragraph.split(" ").length should be > 2
  }
}
