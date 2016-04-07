package org.bjean.sample.wordcount.aws

import java.util

import com.amazonaws.services.elasticmapreduce.model.{BootstrapActionConfig, ScriptBootstrapActionConfig}
import com.typesafe.config.Config
import org.mockito.Mockito._
import org.scalatest.Matchers._
import org.scalatest._
import org.scalatest.mock.MockitoSugar


class BootstrapActionsBuilderTest extends FunSuite with MockitoSugar {


    test("I can Create all Bootstrap with the BootstrapBuilder"){
        val config = mock[Config]


        when(config.getString("aws.cluster.hadoop.bootstrapConfigFile")).thenReturn("s3://cluster-hdfs/bootstrap/config/file")
        when(config.getString("aws.cluster.hadoop.siteConfigFile")).thenReturn("s3://cluster-site/config/file")
        when(config.getString("aws.cluster.spark.bootstrapConfigFile")).thenReturn("s3://cluster-spark/config/file")

        val  bootstraps = new BootstrapActionsBuilder(config).withSpark.withHadoopSiteConfig.withHdfsSiteConfig.build

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



        bootstraps should have size 3
        bootstraps should contain (bootstrapConfigHadoop)
        bootstraps should contain (bootstrapConfigHdfs)
        bootstraps should contain (bootstrapConfigSpark)
    }


    test("I can Create bootstrap action wihtout spark"){
        val config = mock[Config]


        when(config.getString("aws.cluster.hadoop.bootstrapConfigFile")).thenReturn("s3://cluster-hdfs/bootstrap/config/file")
        when(config.getString("aws.cluster.hadoop.siteConfigFile")).thenReturn("s3://cluster-site/config/file")

        val  bootstraps = new BootstrapActionsBuilder(config).withHadoopSiteConfig.withHdfsSiteConfig.build

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


        bootstraps should have size 2
        bootstraps should contain (bootstrapConfigHadoop)
        bootstraps should contain (bootstrapConfigHdfs)

    }

}
