package org.bjean.sample.wordcount.aws

import java.util

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce
import com.amazonaws.services.elasticmapreduce.model._
import com.typesafe.config.Config
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.Matchers._
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mock.MockitoSugar

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import EMRClusterService._

class EMRClusterServiceTest extends FunSuite with MockitoSugar with ScalaFutures {

  val amazonElasticMapReduce = mock[AmazonElasticMapReduce]
  val config = mock[Config]
  val sparkClusterService = new EMRClusterService(amazonElasticMapReduce, config)

  test("A call to createRequest() with valid attributes should return a RunJobFlowRequest") {
    when(config.getString(CONFIG_AWS_CLUSTER_LOG_URI)).thenReturn("s3://this_is_the_log_uri")
    when(config.getString(CONFIG_AWS_CLUSTER_AMI_VERSION)).thenReturn("1.2.3")
    when(config.getString(CONFIG_AWS_CLUSTER_EC2_KEY_NAME)).thenReturn("CONFIG_AWS_CLUSTER_EC2_KEY_NAME")
    when(config.getInt(CONFIG_AWS_CLUSTER_INSTANCE_COUNT)).thenReturn(5)
    when(config.getBoolean(CONFIG_AWS_CLUSTER_KEEP_ALIVE)).thenReturn(true)
    when(config.getBoolean(CONFIG_AWS_CLUSTER_TERMINATION_PROTECTED)).thenReturn(true)
    when(config.getString(CONFIG_AWS_CLUSTER_MASTER_INSTANCE_TYPE)).thenReturn("m1.medium")
    when(config.getString(CONFIG_AWS_CLUSTER_SLAVE_INSTANCE_TYPE)).thenReturn("m1.large")

    val request = sparkClusterService.createRequest("Test Request", List(), List())
    request.getName shouldBe "Test Request"
    request.getLogUri shouldBe "s3://this_is_the_log_uri"
    request.getAmiVersion shouldBe "1.2.3"

    val instances = request.getInstances
    instances.getEc2KeyName shouldBe "CONFIG_AWS_CLUSTER_EC2_KEY_NAME"
    instances.getInstanceCount shouldBe 5
    instances.isKeepJobFlowAliveWhenNoSteps shouldBe true
    instances.isTerminationProtected shouldBe true
    instances.getMasterInstanceType shouldBe "m1.medium"
    instances.getSlaveInstanceType shouldBe "m1.large"
  }

  test("A call to createRequest() with an overwritten getBootStrapActions method should only contain the new bootstrap actions") {
    val bootstrapConfigHadoop: BootstrapActionConfig = new BootstrapActionConfig
    val scriptBootstrapActionConfigHadoop: ScriptBootstrapActionConfig = new ScriptBootstrapActionConfig
    scriptBootstrapActionConfigHadoop.setArgs(util.Arrays.asList("--site-config-file", "s3://cluster-site/config/file"))
    scriptBootstrapActionConfigHadoop.setPath("s3://cluster-hadoop/config/file")

    bootstrapConfigHadoop.setName("Spark Cluster Hadoop Site Config")
    bootstrapConfigHadoop.setScriptBootstrapAction(scriptBootstrapActionConfigHadoop)

    val bootstrapConfigCustom: BootstrapActionConfig = new BootstrapActionConfig
    val scriptBootstrapActionConfigCustom: ScriptBootstrapActionConfig = new ScriptBootstrapActionConfig
    scriptBootstrapActionConfigCustom.setArgs(util.Arrays.asList("--core-config-file", "s3://cluster-site/config/file"))
    scriptBootstrapActionConfigCustom.setPath("s3://cluster-hadoop/config/file")

    bootstrapConfigCustom.setName("Spark Cluster Hadoop Site Config")
    bootstrapConfigCustom.setScriptBootstrapAction(scriptBootstrapActionConfigCustom)

    val bootstrapConfigs = List(bootstrapConfigHadoop, bootstrapConfigCustom)

    val sparkClusterServiceWithGetBootStrapActions = new EMRClusterService(amazonElasticMapReduce, config)

    val request = sparkClusterServiceWithGetBootStrapActions.createRequest("Test Request", List(), bootstrapConfigs)

    request.getBootstrapActions.asScala shouldBe bootstrapConfigs
  }

  test("A call to executeWithSpark() should contains MapReduce bootstrap Action") {
    when(config.getString("aws.cluster.hadoop.bootstrapConfigFile")).thenReturn("s3://cluster-hdfs/bootstrap/config/file")
    when(config.getString("aws.cluster.hadoop.siteConfigFile")).thenReturn("s3://cluster-site/config/file")
    when(config.getString("aws.cluster.spark.bootstrapConfigFile")).thenReturn("s3://cluster-spark/config/file")

    val bootstrapConfigHadoop: BootstrapActionConfig = new BootstrapActionConfig
    val scriptBootstrapActionConfigHadoop: ScriptBootstrapActionConfig = new ScriptBootstrapActionConfig
    scriptBootstrapActionConfigHadoop.setArgs(util.Arrays.asList("--core-config-file", "s3://cluster-site/config/file"))
    scriptBootstrapActionConfigHadoop.setPath("s3://cluster-hdfs/bootstrap/config/file")

    bootstrapConfigHadoop.setName("Hadoop Site Config")
    bootstrapConfigHadoop.setScriptBootstrapAction(scriptBootstrapActionConfigHadoop)

    val bootstrapConfigHdfs: BootstrapActionConfig = new BootstrapActionConfig
    val scriptBootstrapActionConfigHdfs: ScriptBootstrapActionConfig = new ScriptBootstrapActionConfig
    scriptBootstrapActionConfigHdfs.setArgs(util.Arrays.asList("--hdfs-key-value", "dfs.permissions=false"))
    scriptBootstrapActionConfigHdfs.setPath("s3://cluster-hdfs/bootstrap/config/file")

    bootstrapConfigHdfs.setName("HDFS Config")
    bootstrapConfigHdfs.setScriptBootstrapAction(scriptBootstrapActionConfigHdfs)

    val bootstrapConfigSpark: BootstrapActionConfig = new BootstrapActionConfig
    val scriptBootstrapActionConfigSpark: ScriptBootstrapActionConfig = new ScriptBootstrapActionConfig
    scriptBootstrapActionConfigSpark.setPath("s3://cluster-spark/config/file")

    bootstrapConfigSpark.setName("Spark Cluster Config")
    bootstrapConfigSpark.setScriptBootstrapAction(scriptBootstrapActionConfigSpark)

    val bootstrapConfigs = List(bootstrapConfigHdfs, bootstrapConfigHadoop, bootstrapConfigSpark)

    val sparkClusterServiceWithGetBootStrapActions = new EMRClusterService(amazonElasticMapReduce, config) {
      override def startEMRCluster(identifier: String, stepsConfigs: List[StepConfig], bootstrapActions: List[BootstrapActionConfig]): Future[RunJobFlowResult] = {
        identifier shouldBe "Test Request"
        stepsConfigs shouldBe List()
        bootstrapActions.toList shouldBe bootstrapConfigs
        Future(new RunJobFlowResult)
      }
    }

    sparkClusterServiceWithGetBootStrapActions.executeWithSpark("Test Request", List())
  }

  test("A call to createRequest() with defined steps should contain those steps") {
    val step1 = new StepConfig("Step1", new HadoopJarStepConfig())
    val step2 = new StepConfig("Step2", new HadoopJarStepConfig())

    val steps = List(step1, step2)

    val request = sparkClusterService.createRequest("Test Request", steps, List())

    request.getSteps.asScala shouldBe steps
  }

  test("A call to startCluster() with defined steps should call the amazon api and return the jboFlowId") {
    //Given
    val step1 = new StepConfig("Step1", new HadoopJarStepConfig())
    val step2 = new StepConfig("Step2", new HadoopJarStepConfig())

    val steps = List(step1, step2)

    val jobFlowResult = new RunJobFlowResult().withJobFlowId("newJobFlow")
    when(amazonElasticMapReduce.runJobFlow(any[RunJobFlowRequest])).thenReturn(jobFlowResult)

    //When
    val result = sparkClusterService.startEMRCluster("Test Request", steps, List())

    //Then
    result.futureValue shouldBe jobFlowResult
    verify(amazonElasticMapReduce).runJobFlow(any[RunJobFlowRequest])
    verifyNoMoreInteractions(amazonElasticMapReduce)
  }
  test("A call to getStepStatus(), will return EmrStepState for the Step") {

    val jobFlowId = "jobFlow1"
    val listStepResult = new ListStepsResult().
      withSteps(new StepSummary().
        withStatus(new StepStatus().
          withState("COMPLETED")))
    when(amazonElasticMapReduce.listSteps(new ListStepsRequest().withClusterId(jobFlowId)))
      .thenReturn(listStepResult)
    val result = sparkClusterService.getStepStatus(jobFlowId)

    result shouldBe List(Completed)
  }

  test("A call to getStepStatus(), will return EmrStepState for all Step") {

    val jobFlowId = "jobFlow1"
    val listStepResult = new ListStepsResult().
      withSteps(new StepSummary().
        withStatus(new StepStatus().
          withState("COMPLETED")),
        new StepSummary().
          withStatus(new StepStatus().
            withState("INTERRUPTED")))
    when(amazonElasticMapReduce.listSteps(new ListStepsRequest().withClusterId(jobFlowId)))
      .thenReturn(listStepResult)
    val result = sparkClusterService.getStepStatus(jobFlowId)

    result shouldBe List(Completed, Interrupted)
  }
}