package dev.farzadski.gold.aggregates;

import static org.apache.spark.sql.functions.*;

import dev.farzadski.core.aggregate.IAggregation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

public class AirportRevenuePerHourAggregation implements IAggregation {
  private static final double FORCE_AIRPORT_RATIO = 1.30; // airport ≥ 30% better
  private static final double AVOID_AIRPORT_RATIO = 0.90; // airport ≤ 10% worse

  @Override
  public String name() {
    return "AIRPORT_REVENUE_PER_HOUR";
  }

  @Override
  public String outputPath() {
    return "data/YellowTaxiTripRecord/gold/recommender/RPH.parquet";
  }

  @Override
  public Boolean isNeedToSaved() {
    return false;
  }

  @Override
  public Dataset<Row> aggregate(Dataset<Row> silver) {
    // ───────────────────── Normalize base fields
    Dataset<Row> base =
        silver
            .withColumn("IS_AIRPORT", col("AIRPORT_PICKUP_FEE_AMOUNT_USD").gt(0).cast("int"))
            .withColumn("HOUR", hour(col("METER_ENGAGED_AT")))
            .withColumn("DAY_OF_WEEK", dayofweek(col("METER_ENGAGED_AT")));

    // ───────────────────── Revenue per active hour
    Dataset<Row> rph =
        base.groupBy("IS_AIRPORT", "HOUR", "DAY_OF_WEEK")
            .agg(
                sum("TOTAL_CHARGE_AMOUNT_USD").alias("TOTAL_REVENUE_USD"),
                sum("TRIP_DURATION_IN_MINUTES").alias("TOTAL_ACTIVE_MINUTES"))
            .withColumn(
                "REVENUE_USD_PER_ACTIVE_HOUR",
                col("TOTAL_REVENUE_USD").divide(col("TOTAL_ACTIVE_MINUTES")).multiply(60));

    // ───────────────────── Compare airport vs city

    WindowSpec w = Window.partitionBy("HOUR", "DAY_OF_WEEK");

    Dataset<Row> enriched =
        rph.withColumn(
                "CITY_REVENUE_USD_PER_HOUR",
                max(when(col("IS_AIRPORT").equalTo(0), col("REVENUE_USD_PER_ACTIVE_HOUR"))).over(w))
            .withColumn(
                "AIRPORT_REVENUE_USD_PER_HOUR",
                max(when(col("IS_AIRPORT").equalTo(1), col("REVENUE_USD_PER_ACTIVE_HOUR"))).over(w))
            .withColumn(
                "AIRPORT_ADVANTAGE_RATIO",
                col("AIRPORT_REVENUE_USD_PER_HOUR").divide(col("CITY_REVENUE_USD_PER_HOUR")));

    // ───────────────────── Decision layer

    return enriched
        .withColumn(
            "RECOMMENDATION",
            when(col("AIRPORT_ADVANTAGE_RATIO").geq(FORCE_AIRPORT_RATIO), lit("FORCE_AIRPORT"))
                .when(col("AIRPORT_ADVANTAGE_RATIO").leq(AVOID_AIRPORT_RATIO), lit("AVOID_AIRPORT"))
                .otherwise(lit("NEUTRAL")))
        // keep only meaningful rows for downstream consumers
        .filter(col("IS_AIRPORT").equalTo(1))
        .select(
            col("HOUR"),
            col("DAY_OF_WEEK"),
            col("AIRPORT_REVENUE_USD_PER_HOUR"),
            col("CITY_REVENUE_USD_PER_HOUR"),
            col("AIRPORT_ADVANTAGE_RATIO"),
            col("RECOMMENDATION"));
  }
}
