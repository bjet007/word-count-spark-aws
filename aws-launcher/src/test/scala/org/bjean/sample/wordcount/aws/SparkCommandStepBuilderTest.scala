package org.bjean.sample.wordcount.aws

import com.typesafe.config.Config
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar

import scala.collection.JavaConversions._

class SparkCommandStepBuilderTest extends FunSuite with MockitoSugar {
    val AWS_CLUSTER_SPARK_STEP_COMMAND = "aws.cluster.spark.stepCommand"
    val AWS_CLUSTER_SPARK_DEPLOY_MODE = "aws.cluster.spark.deployMode"
    val AWS_CLUSTER_SPARK_MASTER = "aws.cluster.spark.master"
    val AWS_CLUSTER_SPARK_DRIVER_MEMORY = "aws.cluster.spark.driverMemory"
    val AWS_CLUSTER_SPARK_EXECUTOR_MEMORY = "aws.cluster.spark.executorMemory"
    val AWS_CLUSTER_SPARK_EXECUTORS = "aws.cluster.spark.executors"
    val AWS_CLUSTER_SCRIPT_RUNNER_LOCATION = "aws.cluster.scriptRunnerLocation"

    test("We can create a Spark Command line for AWS") {
        val config = mock[Config]
        when(config.getString(AWS_CLUSTER_SPARK_STEP_COMMAND)).thenReturn("spark-submit")
        when(config.getString(AWS_CLUSTER_SPARK_DEPLOY_MODE)).thenReturn("cluster")
        when(config.getString(AWS_CLUSTER_SPARK_MASTER)).thenReturn("yarn-cluster")
        when(config.getString(AWS_CLUSTER_SPARK_DRIVER_MEMORY)).thenReturn("1G")
        when(config.getString(AWS_CLUSTER_SPARK_EXECUTOR_MEMORY)).thenReturn("2G")
        when(config.getString(AWS_CLUSTER_SPARK_EXECUTORS)).thenReturn("4")
        when(config.getString(AWS_CLUSTER_SCRIPT_RUNNER_LOCATION)).thenReturn("s3://elasticmapreduce/libs/script-runner/script-runner.jar")

        val step = new SparkCommandStepBuilder(config)
          .withJarLocation("my-jar-location")
          .withMainClass("com.stw.Main")
          .build;


        val programArgument = List(
            "spark-submit",
            "--deploy-mode", "cluster",
            "--master" ,"yarn-cluster",
            "--driver-memory" ,"1G",
            "--executor-memory", "2G",
            "--num-executors","4",
            "--class", "com.stw.Main",
            "my-jar-location").toIterable

        step.getJar should be ("s3://elasticmapreduce/libs/script-runner/script-runner.jar")
        step.getArgs.toIterable should be (programArgument)
    }


    test("We can create a Spark Command line for AWS with extra arguments") {
        val config = mock[Config]
        when(config.getString(AWS_CLUSTER_SPARK_STEP_COMMAND)).thenReturn("spark-submit")
        when(config.getString(AWS_CLUSTER_SPARK_DEPLOY_MODE)).thenReturn("cluster")
        when(config.getString(AWS_CLUSTER_SPARK_MASTER)).thenReturn("yarn-cluster")
        when(config.getString(AWS_CLUSTER_SPARK_DRIVER_MEMORY)).thenReturn("1G")
        when(config.getString(AWS_CLUSTER_SPARK_EXECUTOR_MEMORY)).thenReturn("2G")
        when(config.getString(AWS_CLUSTER_SPARK_EXECUTORS)).thenReturn("4")
        when(config.getString(AWS_CLUSTER_SCRIPT_RUNNER_LOCATION)).thenReturn("s3://elasticmapreduce/libs/script-runner/script-runner.jar")


        val step = new SparkCommandStepBuilder(config)
          .withJarLocation("my-jar-location")
          .withMainClass("org.bjean.sample.spark.Main")
          .withProgramArgs(List("arg1", "arg2"))
          .build


        val programArgument = List(
            "spark-submit",
            "--deploy-mode", "cluster",
            "--master" ,"yarn-cluster",
            "--driver-memory" ,"1G",
            "--executor-memory", "2G",
            "--num-executors","4",
            "--class", "org.bjean.sample.spark.Main",
            "my-jar-location",
            "arg1","arg2").toIterable


        step.getArgs.toIterable should be (programArgument)
    }

}
