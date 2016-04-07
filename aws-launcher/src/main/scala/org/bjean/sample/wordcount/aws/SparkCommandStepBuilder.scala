package org.bjean.sample.wordcount.aws

import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig
import com.typesafe.config.Config

object SparkCommandStepBuilder {
  val AWS_CLUSTER_SPARK_STEP_COMMAND: String = "aws.cluster.spark.stepCommand"
  val AWS_CLUSTER_SPARK_DEPLOY_MODE: String = "aws.cluster.spark.deployMode"
  val AWS_CLUSTER_SPARK_MASTER: String = "aws.cluster.spark.master"
  val AWS_CLUSTER_SPARK_DRIVER_MEMORY: String = "aws.cluster.spark.driverMemory"
  val AWS_CLUSTER_SPARK_EXECUTOR_MEMORY: String = "aws.cluster.spark.executorMemory"
  val AWS_CLUSTER_SPARK_EXECUTORS: String = "aws.cluster.spark.executors"
}

class SparkCommandStepBuilder(config: Config) extends HadoopJarStepConfigBuilder(config) {
  private var mainClass: String = _
  private var jarLocation: String = _
  private var programArgs: List[String] = List()


  def withJarLocation(jarLocation: String): SparkCommandStepBuilder = {
    this.jarLocation = jarLocation
    this
  }

  def withMainClass(mainClass: String): SparkCommandStepBuilder = {
    this.mainClass = mainClass
    this
  }

  def withProgramArgs(programArgs: List[String]): SparkCommandStepBuilder = {
    this.programArgs = programArgs
    this
  }

  def build: HadoopJarStepConfig = {
    val sparkCommandLine = List(
      config.getString(SparkCommandStepBuilder.AWS_CLUSTER_SPARK_STEP_COMMAND),
      "--deploy-mode",
      config.getString(SparkCommandStepBuilder.AWS_CLUSTER_SPARK_DEPLOY_MODE),
      "--master",
      config.getString(SparkCommandStepBuilder.AWS_CLUSTER_SPARK_MASTER),
      "--driver-memory",
      config.getString(SparkCommandStepBuilder.AWS_CLUSTER_SPARK_DRIVER_MEMORY),
      "--executor-memory",
      config.getString(SparkCommandStepBuilder.AWS_CLUSTER_SPARK_EXECUTOR_MEMORY),
      "--num-executors",
      config.getString(SparkCommandStepBuilder.AWS_CLUSTER_SPARK_EXECUTORS),
      "--class",
      mainClass,
      jarLocation)
    val withProgramArg = sparkCommandLine ++ programArgs

    import collection.JavaConverters._
    new HadoopJarStepConfig(getScriptRunnerLocation).withArgs(withProgramArg.asJava)
  }
}
