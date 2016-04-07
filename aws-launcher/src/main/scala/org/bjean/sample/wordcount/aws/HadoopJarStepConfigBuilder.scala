package org.bjean.sample.wordcount.aws

import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig
import com.typesafe.config.Config
import org.bjean.sample.wordcount.aws.HadoopJarStepConfigBuilder.AWS_CLUSTER_SCRIPT_RUNNER_LOCATION


object HadoopJarStepConfigBuilder {
  val AWS_CLUSTER_SCRIPT_RUNNER_LOCATION: String = "aws.cluster.scriptRunnerLocation"
}

abstract class HadoopJarStepConfigBuilder(config: Config) {

  def getScriptRunnerLocation: String = {
    config.getString(AWS_CLUSTER_SCRIPT_RUNNER_LOCATION)
  }

  def build: HadoopJarStepConfig
}