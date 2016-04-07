package org.bjean.sample.wordcount.aws

import com.amazonaws.services.elasticmapreduce.model.ActionOnFailure.CANCEL_AND_WAIT
import com.amazonaws.services.elasticmapreduce.model.{ActionOnFailure, HadoopJarStepConfig}
import com.typesafe.config.Config
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar

import scala.collection.JavaConversions._

class ProcessingStepBuilderTest extends  FunSuite with MockitoSugar{
    val AWS_CLUSTER_SCRIPT_RUNNER_LOCATION = "aws.cluster.scriptRunnerLocation"

    test("I can build a Script Runner builder"){
        val config = mock[Config]
        when(config.getString(AWS_CLUSTER_SCRIPT_RUNNER_LOCATION)).thenReturn("s3://elasticmapreduce/libs/script-runner/script-runner.jar")

        val step = new ProcessingStepBuilder().withName("My Step").withHadoopJarStep(new HadoopJarStepConfigBuilder(config) {

           override def build: HadoopJarStepConfig = {
               new HadoopJarStepConfig("s3://elasticmapreduce/libs/script-runner/script-runner.jar").withArgs(List("Arg1","Arg2"))
           }
       }).build

       step.getActionOnFailure should be (ActionOnFailure.TERMINATE_JOB_FLOW.name())
       step.getName should be ("My Step")
       step.getHadoopJarStep.getJar should be ("s3://elasticmapreduce/libs/script-runner/script-runner.jar")
       step.getHadoopJarStep.getArgs should contain ("Arg1")
       step.getHadoopJarStep.getArgs should contain ("Arg2")

    }

    test("I can build a Script Runner builder with no Args"){
        intercept[IllegalArgumentException] {
            val step = new ProcessingStepBuilder().withName("My Step").build
        }
    }

    test("I can build a Script Runner builder with specific ActionOnFailure"){
        val config = mock[Config]
        when(config.getString(AWS_CLUSTER_SCRIPT_RUNNER_LOCATION)).thenReturn("s3://elasticmapreduce/libs/script-runner/script-runner.jar")

        val step = new ProcessingStepBuilder().withName("My Step").withActionOnFailure(CANCEL_AND_WAIT).withHadoopJarStep(new HadoopJarStepConfigBuilder(config) {
            override def build: HadoopJarStepConfig = {
                new HadoopJarStepConfig("s3://elasticmapreduce/libs/script-runner/script-runner.jar")
            }
        }).build

        step.getActionOnFailure should be (CANCEL_AND_WAIT.name())
        step.getName should be ("My Step")
        step.getHadoopJarStep.getArgs should be ('empty)
        step.getHadoopJarStep.getJar should be ("s3://elasticmapreduce/libs/script-runner/script-runner.jar")
    }



}
