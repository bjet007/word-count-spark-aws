package org.bjean.sample.wordcount.aws


import com.amazonaws.services.elasticmapreduce.model.BootstrapActionConfig
import com.amazonaws.services.elasticmapreduce.model.ScriptBootstrapActionConfig
import com.typesafe.config.Config
import org.bjean.sample.wordcount.aws.BootstrapActionsBuilder._

import scala.collection.mutable


object BootstrapActionsBuilder {
  val CONFIG_AWS_CLUSTER_HADOOP_SITE_CONFIG_FILE: String = "aws.cluster.hadoop.siteConfigFile"
  val CONFIG_AWS_CLUSTER_SPARK_INSTALL_FILE: String = "aws.cluster.spark.bootstrapConfigFile"
  val CONFIG_AWS_CLUSTER_HADOOP_BOOTSTRAP_CONFIG_FILE: String = "aws.cluster.hadoop.bootstrapConfigFile"

  def createBootstrapAction(bootstrapName: String, bootstrapPath: String, args: List[String]): BootstrapActionConfig = {
    import collection.JavaConversions._
    val bootstrapScriptConfig: ScriptBootstrapActionConfig = new ScriptBootstrapActionConfig
    bootstrapScriptConfig.setPath(bootstrapPath)
    if (args != null) {
      bootstrapScriptConfig.setArgs(args)
    }
    val bootstrapConfig: BootstrapActionConfig = new BootstrapActionConfig
    bootstrapConfig.setName(bootstrapName)
    bootstrapConfig.setScriptBootstrapAction(bootstrapScriptConfig)
    bootstrapConfig
  }
}

class BootstrapActionsBuilder(config: Config) {
  private var sparkConfig: Boolean = false
  private var hadoopSiteConfig: Boolean = false
  private var hdfsSiteConfig: Boolean = false
  private var customBootstrapActionConfigs: List[BootstrapActionConfig] = null

  def withSpark: BootstrapActionsBuilder = {
    sparkConfig = true
    this
  }

  def withHadoopSiteConfig: BootstrapActionsBuilder = {
    hadoopSiteConfig = true
    this
  }

  def withHdfsSiteConfig: BootstrapActionsBuilder = {
    hdfsSiteConfig = true
    this
  }

  def withCustomBootstrapActions(bootstrapActionConfigs: List[BootstrapActionConfig]): BootstrapActionsBuilder = {
    customBootstrapActionConfigs = bootstrapActionConfigs
    this
  }

  def build: List[BootstrapActionConfig] = {
    val bootstrapActionConfigs: mutable.MutableList[BootstrapActionConfig] = mutable.MutableList()
    if (hdfsSiteConfig) {
      bootstrapActionConfigs.+=(createHdfsSiteConfig)
    }
    if (hadoopSiteConfig) {
      bootstrapActionConfigs.+=(createHadoopSiteConfig)
    }
    if (sparkConfig) {
      bootstrapActionConfigs.+=(createSparkConfig)
    }
    if (customBootstrapActionConfigs != null) {
      for(custom <- customBootstrapActionConfigs){
        bootstrapActionConfigs.+=(custom)
      }
    }
    bootstrapActionConfigs.toList
  }

  protected def createHadoopSiteConfig: BootstrapActionConfig = {
    val siteBootstrapConfigArgs = List("--core-config-file",config.getString(CONFIG_AWS_CLUSTER_HADOOP_SITE_CONFIG_FILE))
    createBootstrapAction("Hadoop Site Config", config.getString(CONFIG_AWS_CLUSTER_HADOOP_BOOTSTRAP_CONFIG_FILE), siteBootstrapConfigArgs)
  }

  protected def createHdfsSiteConfig: BootstrapActionConfig = {
    val hdfsBootstrapConfigArgs = List("--hdfs-key-value","dfs.permissions=false")
    createBootstrapAction("HDFS Config", config.getString(CONFIG_AWS_CLUSTER_HADOOP_BOOTSTRAP_CONFIG_FILE), hdfsBootstrapConfigArgs)
  }

  protected def createSparkConfig: BootstrapActionConfig = {
    createBootstrapAction("Spark Cluster Config", config.getString(CONFIG_AWS_CLUSTER_SPARK_INSTALL_FILE), null)
  }
}
