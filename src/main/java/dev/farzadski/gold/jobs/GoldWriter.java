package dev.farzadski.gold.jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class GoldWriter {
  private GoldWriter() {}

  public static void write(Dataset<Row> df, String path) {
    df.repartition(functions.col("TRIP_DATE"))
        .write()
        .mode("overwrite")
        .option("header", "true") // ✅ this is what you want
        .partitionBy("TRIP_DATE")
        .csv(path);
  }
}
