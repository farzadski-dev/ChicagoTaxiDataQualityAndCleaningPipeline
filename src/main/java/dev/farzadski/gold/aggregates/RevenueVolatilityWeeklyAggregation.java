package dev.farzadski.gold.aggregates;

import static org.apache.spark.sql.functions.*;

import dev.farzadski.core.aggregate.IAggregation;
import dev.farzadski.gold.enums.ColumnName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

public final class RevenueVolatilityWeeklyAggregation implements IAggregation {

  // ────────────────────── Dimension
  private static final String WEEK_START_DATE = "WEEK_START_DATE";

  // ────────────────────── Measure
  private static final String REVENUE_USD = "REVENUE_USD";

  // ────────────────────── Baseline
  private static final String BASELINE_MEAN_USD = "BASELINE_WEEKLY_REVENUE_MEAN_USD";
  private static final String BASELINE_SPREAD_USD = "BASELINE_WEEKLY_REVENUE_SPREAD_USD";

  // ────────────────────── Expected bounds
  private static final String EXPECTED_LOW_USD = "EXPECTED_WEEKLY_REVENUE_LOW_USD";
  private static final String EXPECTED_HIGH_USD = "EXPECTED_WEEKLY_REVENUE_HIGH_USD";

  // ────────────────────── Deviation / anomaly
  private static final String DEVIATION_SCORE = "WEEKLY_REVENUE_DEVIATION_SCORE";
  private static final String IS_ANOMALY = "IS_WEEKLY_REVENUE_ANOMALY";
  private static final String DEVIATION_LEVEL = "DEVIATION_LEVEL";

  private static final int ROLLING_OBSERVATIONS = 8; // weeks
  private static final int SIGMA_MULTIPLIER = 2;

  @Override
  public String name() {
    return "REVENUE_VOLATILITY_WEEKLY";
  }

  @Override
  public String outputPath() {
    return "data/YellowTaxiTripRecord/gold/aggregates/REVENUE_VOLATILITY_WEEKLY.csv";
  }

  @Override
  public Boolean isNeedToSaved() {
    return false;
  }

  @Override
  public Dataset<Row> aggregate(Dataset<Row> silver) {

    // ────────────────────── Week derivation (ISO-style, week starts Monday)
    var engagedAt = col(ColumnName.METER_ENGAGED_AT.canonical());

    Dataset<Row> weeklyRevenue =
        silver
            .withColumn(WEEK_START_DATE, date_trunc("week", engagedAt).cast("date"))
            .groupBy(WEEK_START_DATE)
            .agg(sum(col(ColumnName.TOTAL_CHARGE_AMOUNT_USD.canonical())).alias(REVENUE_USD));

    // ────────────────────── Rolling weekly baseline
    WindowSpec rollingBaselineWindow =
        Window.orderBy(col(WEEK_START_DATE)).rowsBetween(-ROLLING_OBSERVATIONS, -1);

    Dataset<Row> withBaseline =
        weeklyRevenue
            .withColumn(BASELINE_MEAN_USD, avg(REVENUE_USD).over(rollingBaselineWindow))
            .withColumn(BASELINE_SPREAD_USD, stddev(REVENUE_USD).over(rollingBaselineWindow));

    // ────────────────────── Expected bounds + anomaly logic
    return withBaseline
        .withColumn(
            EXPECTED_LOW_USD,
            col(BASELINE_MEAN_USD).minus(col(BASELINE_SPREAD_USD).multiply(SIGMA_MULTIPLIER)))
        .withColumn(
            EXPECTED_HIGH_USD,
            col(BASELINE_MEAN_USD).plus(col(BASELINE_SPREAD_USD).multiply(SIGMA_MULTIPLIER)))
        .withColumn(
            DEVIATION_SCORE,
            when(col(BASELINE_SPREAD_USD).isNull().or(col(BASELINE_SPREAD_USD).equalTo(0)), lit(0))
                .otherwise(
                    abs(col(REVENUE_USD).minus(col(BASELINE_MEAN_USD)))
                        .divide(col(BASELINE_SPREAD_USD))))
        .withColumn(
            IS_ANOMALY,
            col(REVENUE_USD)
                .lt(col(EXPECTED_LOW_USD))
                .or(col(REVENUE_USD).gt(col(EXPECTED_HIGH_USD))))
        .withColumn(
            DEVIATION_LEVEL,
            when(col(DEVIATION_SCORE).gt(3), "EXTREME")
                .when(col(DEVIATION_SCORE).gt(2), "HIGH")
                .when(col(DEVIATION_SCORE).gt(1), "MEDIUM")
                .otherwise("NORMAL"))
        .orderBy(col(WEEK_START_DATE));
  }
}
