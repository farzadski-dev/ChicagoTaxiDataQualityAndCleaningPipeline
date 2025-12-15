package dev.farzadski.core.aggregate;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface IAggregation {
  String name();

  Dataset<Row> aggregate(Dataset<Row> input);

  String outputPath();

  Boolean isNeedToSaved();
}
