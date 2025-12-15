package dev.farzadski.shared.profiling;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class FrequencyTablePrinter {
  private FrequencyTablePrinter() {}

  public static void print(Dataset<Row> df, String columnName) {

    Dataset<Row> total = df.agg(functions.count(functions.lit(1)).alias("TOTAL_COUNT"));

    Dataset<Row> freq = df.groupBy(functions.col(columnName)).count();

    Dataset<Row> result =
        freq.crossJoin(total)
            .withColumn(
                "PERCENTAGE",
                functions.round(
                    functions.col("COUNT").multiply(100.0).divide(functions.col("TOTAL_COUNT")), 2))
            .drop("TOTAL_COUNT")
            .orderBy(functions.desc("COUNT"));

    System.out.println("\n=== FREQUENCY_TABLE: " + columnName + " ===");
    result.show(false);
  }
}
