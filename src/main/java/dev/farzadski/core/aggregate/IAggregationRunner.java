package dev.farzadski.core.aggregate;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface IAggregationRunner {
  void run(Dataset<Row> silver);
}
