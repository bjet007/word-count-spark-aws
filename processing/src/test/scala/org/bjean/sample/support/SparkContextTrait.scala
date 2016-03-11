package org.bjean.sample.support

import java.nio.file.{Files, Paths}

import com.typesafe.config.{Config, ConfigValueFactory}
import org.apache.spark.{SparkConf, SparkContext}

trait SparkContextTrait {

    def withSparkContext(name: String = "test")(body : (SparkContext) => Unit) = {
        System.clearProperty("spark.driver.port")
        System.clearProperty("spark.driver.host")
        System.setProperty("hadoop.home.dir", "tmp/spark-hadoop-test")

        val conf = new SparkConf().setMaster("local[1]").setAppName(name)
        val ssc = new SparkContext(conf)
        try {
            body(ssc)
        } finally {
            ssc.stop()
            System.clearProperty("spark.driver.port")
            System.clearProperty("spark.driver.host")
        }
    }
}
