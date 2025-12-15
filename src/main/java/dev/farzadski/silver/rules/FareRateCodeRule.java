package dev.farzadski.silver.rules;

import dev.farzadski.bronze.enums.ColumnName;
import dev.farzadski.core.rules.IRule;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class FareRateCodeRule implements IRule<Dataset<Row>> {
  private static final long UNKNOWN_RATE_CODE = 99L;
  private static final String TARGET_COLUMN = ColumnName.FARE_RATE_CODE.canonical();

  private static final Set<Long> ALLOWED =
      Set.of(
          1L, // Standard rate
          2L, // JFK
          3L, // Newark
          4L, // Nassau or Westchester
          5L, // Negotiated fare
          6L // Group ride
          );

  @Override
  public Dataset<Row> apply(Dataset<Row> input) {
    return input.withColumn(
        TARGET_COLUMN,
        functions
            .when(
                functions
                    .col(TARGET_COLUMN)
                    .isNull()
                    .or(functions.not(functions.col(TARGET_COLUMN).isin(ALLOWED.toArray()))),
                UNKNOWN_RATE_CODE)
            .otherwise(functions.col(TARGET_COLUMN)));
  }
}
