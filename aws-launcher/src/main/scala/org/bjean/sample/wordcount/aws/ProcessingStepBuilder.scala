package org.bjean.sample.wordcount.aws

import com.amazonaws.services.elasticmapreduce.model.ActionOnFailure.TERMINATE_JOB_FLOW
import com.amazonaws.services.elasticmapreduce.model.{StepConfig, ActionOnFailure}


class ProcessingStepBuilder {
  private var name: String = null
  private var actionOnFailure = TERMINATE_JOB_FLOW
  private var builder: HadoopJarStepConfigBuilder = null

  def withName(name: String): ProcessingStepBuilder = {
    this.name = name
    this
  }

  def withActionOnFailure(actionOnFailure: ActionOnFailure): ProcessingStepBuilder = {
    this.actionOnFailure = actionOnFailure
    this
  }

  def withHadoopJarStep(builder: HadoopJarStepConfigBuilder): ProcessingStepBuilder = {
    this.builder = builder
    this
  }

  def build: StepConfig = {
    if (builder == null) {
      throw new IllegalArgumentException("Can't build a Step with JarStep Builder")
    }
    new StepConfig().withName(name).withActionOnFailure(actionOnFailure).withHadoopJarStep(builder.build)
  }
}