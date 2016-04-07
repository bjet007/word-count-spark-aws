package org.bjean.sample.wordcount.aws


object EmrStepState {

  def fromString(value: String): Option[EmrStepState] = {
    Vector(Pending, Running, Completed, Canceled, Failed, Interrupted).find(_.awsSdkValue == value)
  }

  def allCompleted(steps: List[EmrStepState]): Boolean = steps match {
    case Nil => true
    case x :: xs => x.completed && allCompleted(xs)
  }

  def allSuccess(steps: List[EmrStepState]): Boolean = steps match {
    case Nil => true
    case x :: xs => x.success && allSuccess(xs)
  }
}

sealed trait EmrStepState {
  def awsSdkValue: String

  def completed: Boolean

  def success: Boolean


}

case object Pending extends EmrStepState {
  val awsSdkValue = "PENDING"
  val completed = false
  val success = false
}

case object Running extends EmrStepState {
  val awsSdkValue = "RUNNING"
  val completed = false
  val success = false
}

case object Completed extends EmrStepState {
  val awsSdkValue = "COMPLETED"
  val completed = true
  val success = true
}

case object Canceled extends EmrStepState {
  val awsSdkValue = "CANCELLED"
  val completed = true
  val success = false
}

case object Failed extends EmrStepState {
  val awsSdkValue = "FAILED"
  val completed = true
  val success = false
}

case object Interrupted extends EmrStepState {
  val awsSdkValue = "INTERRUPTED"
  val completed = true
  val success = false
}
