package dev.farzadski.gold.aggregates;

import static org.apache.spark.sql.functions.*;

import dev.farzadski.core.aggregate.IAggregation;
import org.apache.spark.sql.*;

public class ShiftChangeSimulationAggregation implements IAggregation {

  @Override
  public String name() {
    return "SHIFT_CHANGE_SIMULATION";
  }

  @Override
  public String outputPath() {
    return "data/YellowTaxiTripRecord/gold/recommender/SHIFT_IMPACT.parquet";
  }

  @Override
  public Boolean isNeedToSaved() {
    return false;
  }

  @Override
  public Dataset<Row> aggregate(Dataset<Row> silver) {
    Dataset<Row> hourly =
        silver
            .withColumn("HOUR", hour(col("METER_ENGAGED_AT")))
            .groupBy("HOUR")
            .agg(count(lit(1)).alias("TRIPS"), avg("TOTAL_CHARGE_AMOUNT_USD").alias("AVERGE_REV"));

    return hourly
        .withColumn(
            "SHIFT_AT_3PM_GAIN",
            when(col("HOUR").between(15, 18), col("TRIPS").multiply(1.15)).otherwise(col("TRIPS")))
        .withColumn(
            "SHIFT_AT_6PM_GAIN",
            when(col("HOUR").between(18, 21), col("TRIPS").multiply(1.25)).otherwise(col("TRIPS")));
  }
}
