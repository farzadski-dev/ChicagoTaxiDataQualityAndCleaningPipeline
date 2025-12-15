package dev.farzadski.silver.rules;

import dev.farzadski.core.rules.IRule;
import dev.farzadski.silver.enums.ColumnName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class PassengerCountRule implements IRule<Dataset<Row>> {
  @Override
  public Dataset<Row> apply(Dataset<Row> input) {

    return input
        // preserve missingness information (0 / 1 flag)
        .withColumn(
            ColumnName.IS_PASSENGER_COUNT_MISSING.canonical(),
            functions
                .when(
                    functions.col(ColumnName.PASSENGER_COUNT.canonical()).isNull(),
                    functions.lit(1))
                .otherwise(functions.lit(0)))
        // normalize nulls
        .withColumn(
            ColumnName.PASSENGER_COUNT.canonical(),
            functions.coalesce(
                functions.col(ColumnName.PASSENGER_COUNT.canonical()), functions.lit(0)));
  }
}
