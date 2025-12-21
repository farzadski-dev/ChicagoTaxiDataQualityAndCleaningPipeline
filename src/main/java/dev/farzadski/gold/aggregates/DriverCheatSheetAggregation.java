package dev.farzadski.gold.aggregates;

import static org.apache.spark.sql.functions.*;

import dev.farzadski.core.aggregate.IAggregation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

public class DriverCheatSheetAggregation implements IAggregation {

  @Override
  public String name() {
    return "DRIVER_CHEAT_SHEET";
  }

  @Override
  public String outputPath() {
    return "data/YellowTaxiTripRecord/gold/recommender/DRIVER_CHEAT_SHEET.parquet";
  }

  @Override
  public Boolean isNeedToSaved() {
    return false;
  }

  @Override
  public Dataset<Row> aggregate(Dataset<Row> silver) {
    WindowSpec hourWindow = Window.partitionBy("PICKUP_HOUR_OF_DAY");

    WindowSpec zoneTimeWindow =
        Window.partitionBy("PICKUP_TAXI_ZONE_ID").orderBy("PICKUP_HOUR_OF_DAY");

    return silver
        .withColumn("PICKUP_HOUR_OF_DAY", hour(col("METER_ENGAGED_AT")))
        .groupBy("PICKUP_TAXI_ZONE_ID", "PICKUP_HOUR_OF_DAY")
        .agg(
            count(lit(1)).alias("TRIP_REQUEST_COUNT"),
            avg("TOTAL_CHARGE_AMOUNT_USD").alias("AVERAGE_TRIP_REVENUE_USD"),
            avg("TIP_AMOUNT_USD").alias("AVERAGE_TIP_USD"))

        // ───── Earning signal
        .withColumn(
            "ZONE_EARNING_SCORE",
            col("TRIP_REQUEST_COUNT")
                .multiply(col("AVERAGE_TRIP_REVENUE_USD").plus(col("AVERAGE_TIP_USD"))))

        // ───── Sample size guard
        .withColumn(
            "EXPECTED_EARNINGS_PER_HOUR_USD",
            when(col("TRIP_REQUEST_COUNT").geq(30), col("ZONE_EARNING_SCORE")))

        // ───── Percentile normalization
        .withColumn(
            "ZONE_EARNING_PERCENTILE",
            percent_rank().over(hourWindow.orderBy(col("EXPECTED_EARNINGS_PER_HOUR_USD"))))
        .withColumn("ZONE_SCORE_0_100", col("ZONE_EARNING_PERCENTILE").multiply(100))

        // ───── Hour-ahead look
        .withColumn(
            "NEXT_HOUR_EXPECTED_EARNINGS",
            lead(col("EXPECTED_EARNINGS_PER_HOUR_USD"), 1).over(zoneTimeWindow))

        // ───── Action
        .withColumn(
            "TIME_ACTION_RECOMMENDATION",
            when(
                    col("NEXT_HOUR_EXPECTED_EARNINGS")
                        .gt(col("EXPECTED_EARNINGS_PER_HOUR_USD").multiply(1.10)),
                    lit("WAIT_15_MIN"))
                .when(
                    col("NEXT_HOUR_EXPECTED_EARNINGS")
                        .lt(col("EXPECTED_EARNINGS_PER_HOUR_USD").multiply(0.90)),
                    lit("MOVE_NOW"))
                .otherwise(lit("NEUTRAL")));
  }
}
