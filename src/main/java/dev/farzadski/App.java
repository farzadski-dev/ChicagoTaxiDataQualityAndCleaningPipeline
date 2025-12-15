package dev.farzadski;

import dev.farzadski.gold.jobs.GoldJob;
import dev.farzadski.shared.spark.SparkUtils;
import org.apache.spark.sql.SparkSession;

public class App {

  public static void main(String[] args) {
    SparkSession spark = SparkUtils.build("CsvIngestJob");

    System.out.println("CONNECTING_TO_SPARK");
    //    new BronzeJob(spark).run();
    //    new SilverJob(spark).run();
    new GoldJob(spark).run();

    spark.stop();
  }
}
