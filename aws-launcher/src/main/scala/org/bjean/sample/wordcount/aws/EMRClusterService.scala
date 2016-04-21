package org.bjean.sample.wordcount.aws

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce
import com.amazonaws.services.elasticmapreduce.model._
import com.typesafe.config.Config
import EMRClusterService._

import scala.concurrent.{ExecutionContext, Future}

object EMRClusterService {
  val CONFIG_AWS_CLUSTER_LOG_URI = "aws.cluster.logUri"
  val CONFIG_AWS_CLUSTER_AMI_VERSION = "aws.cluster.amiVersion"
  val CONFIG_AWS_CLUSTER_EC2_KEY_NAME = "aws.cluster.ec2KeyName"
  val CONFIG_AWS_CLUSTER_INSTANCE_COUNT = "aws.cluster.instanceCount"
  val CONFIG_AWS_CLUSTER_TERMINATION_PROTECTED = "aws.cluster.terminationProtected"
  val CONFIG_AWS_CLUSTER_MASTER_INSTANCE_TYPE = "aws.cluster.masterInstanceType"
  val CONFIG_AWS_CLUSTER_SLAVE_INSTANCE_TYPE = "aws.cluster.slaveInstanceType"
  val CONFIG_AWS_CLUSTER_KEEP_ALIVE = "aws.cluster.keepAlive"
  
}


class EMRClusterService(elasticMapReduceClient: AmazonElasticMapReduce, config: Config)(implicit ec: ExecutionContext) {
  def executeWithSpark(identifier: String, stepsConfigs: List[StepConfig]): Future[RunJobFlowResult] = {
    val bootstrapActionConfigs = new BootstrapActionsBuilder(config).withSpark.withHadoopSiteConfig.withHdfsSiteConfig.build
    startEMRCluster(identifier, stepsConfigs, bootstrapActionConfigs.toList)
  }


  def startEMRCluster(identifier: String, stepsConfigs: List[StepConfig], bootstrapActions: List[BootstrapActionConfig]): Future[RunJobFlowResult] = {
    val request: RunJobFlowRequest = createRequest(identifier, stepsConfigs, bootstrapActions)
    Future {
      elasticMapReduceClient.runJobFlow(request)
    }
  }

  def createRequest(identifier: String, stepsConfigs: List[StepConfig], bootstrapActions: List[BootstrapActionConfig]): RunJobFlowRequest = {
    import collection.JavaConverters._
    new RunJobFlowRequest()
      .withBootstrapActions(bootstrapActions.asJavaCollection)
      .withName(identifier)
      .withLogUri(config.getString(CONFIG_AWS_CLUSTER_LOG_URI))
      .withAmiVersion(config.getString(CONFIG_AWS_CLUSTER_AMI_VERSION))
      .withVisibleToAllUsers(true)
      .withInstances(new JobFlowInstancesConfig().withEc2KeyName(config.getString(CONFIG_AWS_CLUSTER_EC2_KEY_NAME))
        .withInstanceCount(config.getInt(CONFIG_AWS_CLUSTER_INSTANCE_COUNT))
        .withKeepJobFlowAliveWhenNoSteps(config.getBoolean(CONFIG_AWS_CLUSTER_KEEP_ALIVE))
        .withTerminationProtected(config.getBoolean(CONFIG_AWS_CLUSTER_TERMINATION_PROTECTED))
        .withMasterInstanceType(config.getString(CONFIG_AWS_CLUSTER_MASTER_INSTANCE_TYPE))
        .withSlaveInstanceType(config.getString(CONFIG_AWS_CLUSTER_SLAVE_INSTANCE_TYPE)))
      .withSteps(stepsConfigs.asJavaCollection)
  }

  def getStepStatus(jobFlowId: String): Future[List[EmrStepState]] = {
    val futureSteps = Future {
       elasticMapReduceClient.listSteps(new ListStepsRequest().withClusterId(jobFlowId))
    }
    
    futureSteps.flatMap( result => Future {
        if (result == null) {
          throw new IllegalArgumentException(String.format("Job flow Id [%s] doesn't not exist", jobFlowId))
        }
        if (result.getSteps == null) {
          throw new IllegalArgumentException(String.format("Job flow Id [%s] does not have any steps", jobFlowId))
        }
        import collection.JavaConverters._
        val r = for (s <- result.getSteps.asScala) yield EmrStepState.fromString(s.getStatus.getState)
        r.flatten.toList
    })
  }
}