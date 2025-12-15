package dev.farzadski.shared.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkUtils {

  public static SparkSession build(String appName) {
    SparkConf conf =
        new SparkConf()
            .setAppName(appName)
            .setMaster("local[*]")
            .set("spark.driver.extraClassPath", System.getProperty("java.class.path"))
            .set("spark.executor.extraClassPath", System.getProperty("java.class.path"));

    var spark = SparkSession.builder().config(conf).getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    spark.conf().set("spark.sql.execution.arrow.pyspark.enabled", "false");

    return spark;
  }
}
