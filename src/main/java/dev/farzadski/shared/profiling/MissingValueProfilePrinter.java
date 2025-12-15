package dev.farzadski.shared.profiling;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class MissingValueProfilePrinter {
  private MissingValueProfilePrinter() {}

  public static void print(Dataset<Row> df) {

    long totalRows = df.count();

    Dataset<Row> result = null;

    for (String colName : df.columns()) {

      Dataset<Row> stats =
          df.select(
                  functions.lit(colName).alias("COLUMN_NAME"),
                  functions
                      .sum(functions.when(functions.col(colName).isNull(), 1).otherwise(0))
                      .alias("NULL_COUNT"),
                  functions
                      .sum(
                          functions
                              .when(
                                  functions.trim(functions.col(colName).cast("string")).equalTo(""),
                                  1)
                              .otherwise(0))
                      .alias("EMPTY_STRING_COUNT"))
              .withColumn(
                  "NULL_PERCENTAGE",
                  functions.round(functions.col("NULL_COUNT").multiply(100.0).divide(totalRows), 2))
              .withColumn(
                  "EMPTY_PERCENTAGE",
                  functions.round(
                      functions.col("EMPTY_STRING_COUNT").multiply(100.0).divide(totalRows), 2));

      result = (result == null) ? stats : result.union(stats);
    }

    System.out.println("\n=== Null & Empty Value Report ===");
    result.orderBy(functions.desc("NULL_COUNT"), functions.desc("EMPTY_STRING_COUNT")).show(false);
  }
}
