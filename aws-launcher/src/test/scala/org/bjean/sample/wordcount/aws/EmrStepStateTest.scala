package org.bjean.sample.wordcount.aws

import org.bjean.sample.wordcount.aws.EmrStepState.{allCompleted, allSuccess}
import org.scalatest.Matchers._
import org.scalatest._
import org.scalatest.mock.MockitoSugar


class EmrStepStateTest extends FunSuite with MockitoSugar {

  test("When all state are complete, the global state is completed") {
    val states = List(Completed, Interrupted)
    val result = allCompleted(states)

    result shouldBe true
  }

  test("When there is a running step, the global state is  incompleted") {
    val states = List(Completed, Running)
    val result = allCompleted(states)

    result shouldBe false
  }

  test("When all state are success, the global state is success") {
    val states = List(Completed, Completed)
    val result = allSuccess(states)

    result shouldBe true
  }

  test("When there is a failed step, the global state is not successful") {
    val states = List(Completed, Interrupted)
    val result = allSuccess(states)

    result shouldBe false
  }
}
