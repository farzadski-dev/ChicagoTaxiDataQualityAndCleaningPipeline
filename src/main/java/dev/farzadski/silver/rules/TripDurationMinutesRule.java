package dev.farzadski.silver.rules;

import dev.farzadski.core.rules.IRule;
import dev.farzadski.silver.enums.ColumnName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class TripDurationMinutesRule implements IRule<Dataset<Row>> {

  @Override
  public Dataset<Row> apply(Dataset<Row> input) {

    String startCol = ColumnName.METER_ENGAGED_AT.canonical();
    String endCol = ColumnName.METER_DISENGAGED_AT.canonical();
    String durationCol = ColumnName.TRIP_DURATION_IN_MINUTES.canonical();

    return input
        // compute duration in minutes using column references
        .withColumn(
        durationCol,
        functions.round(
            functions
                .unix_timestamp(functions.col(endCol))
                .minus(functions.unix_timestamp(functions.col(startCol)))
                .divide(60.0),
            2));
  }
}
