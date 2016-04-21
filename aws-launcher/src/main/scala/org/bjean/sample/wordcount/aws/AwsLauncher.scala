package org.bjean.sample.wordcount.aws

import java.nio.file.{Path, Paths}
import java.util.concurrent.Executors

import akka.actor.ActorSystem
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult
import com.typesafe.config.Config
import org.bjean.sample.wordcount.aws.EmrStepState.allCompleted
import org.bjean.sample.wordcount.aws.support.Retrying

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

class AwsLauncher(config: Config, sparkS3Services: S3Services, sparkClusterService: EMRClusterService, showDefinition: Path, processJar: Path, className: String, outputPath: String, sessionsPath: String) extends Retrying {
  val AWS_CLUSTER_NAME: String = "aws.cluster.name"
  val AWS_CLUSTER_STEP_STATUS_CHECK_MILLIS: String = "aws.cluster.stepStatusCheckMillis"
  val CONFIG_HADOOP_FS_S3N_AWS_ACCESS_KEY_ID: String = "aws.fs.s3.awsAccessKeyId"
  val CONFIG_HADOOP_FS_S3N_AWS_SECRET_ACCESS_KEY: String = "aws.fs.s3.awsSecretAccessKey"

  implicit val ec = ExecutionContext.fromExecutor(
    Executors.newFixedThreadPool(4))
  val system = ActorSystem("My System")
  val delay: FiniteDuration = 30 seconds
  
  def execute() :Unit= {
    val executionResult = sparkS3Services.getExecutionContextPath("result")
    
    val eventualJobFlowResult = for {
                                  s3ShowDefinition <- sparkS3Services.copyExecutionContextFileToS3(showDefinition)
                                  s3processJar <- sparkS3Services.copyExecutionContextFileToS3(processJar)
                                  sparkCommandBuilder <- Future(new SparkCommandStepBuilder(config).withJarLocation(s3processJar.toS3Path).withMainClass(className).withProgramArgs(programArguments(s3ShowDefinition, executionResult)))
                                  step <- Future(new ProcessingStepBuilder().withName("RunScript").withHadoopJarStep(sparkCommandBuilder).build)
                                  jobFlow <- sparkClusterService.executeWithSpark(config.getString(AWS_CLUSTER_NAME), List(step))
                                } yield jobFlow

    val localResult = for {
                        jobFlowResult <- eventualJobFlowResult
                        complete <- retry(retrieveStatus(jobFlowResult.getJobFlowId), delay, 100)(ec, system.scheduler)
                        result <- sparkS3Services.copyResultToLocalPath(new S3NativeFile(executionResult.bucket, executionResult.key + "/part-00000"), Paths.get(outputPath))
                      } yield result
    

    localResult.onSuccess{ case downwload =>  println(s"${downwload.toString}")}
  }

  def retrieveStatus(jobFlowId: String):Future[String] = {
    val  stepStates = sparkClusterService.getStepStatus(jobFlowId)
    stepStates.map{
      case steps if allCompleted(steps) => jobFlowId
      case _ => throw new Exception("incomplete") }
  }
  
  private def programArguments(showDefinition: S3File, executionResult: S3File): List[String] = {
    List(
      "--awsAccessKeyId",
      config.getString(CONFIG_HADOOP_FS_S3N_AWS_ACCESS_KEY_ID),
      "--awsSecretAccessKey",
      config.getString(CONFIG_HADOOP_FS_S3N_AWS_SECRET_ACCESS_KEY),
      "--inputFile",
      showDefinition.toS3Path,
      "--outputPath",
      executionResult.toS3Path)
  }
}
