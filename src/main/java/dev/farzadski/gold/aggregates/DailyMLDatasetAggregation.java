package dev.farzadski.gold.aggregates;

import static org.apache.spark.sql.functions.*;

import dev.farzadski.core.aggregate.IAggregation;
import dev.farzadski.gold.enums.ColumnName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

public final class DailyMLDatasetAggregation implements IAggregation {

  private static final String TRIP_DATE = "TRIP_DATE";
  private static final String DAILY_REVENUE = "DAILY_REVENUE_USD";
  private static final String TRIP_COUNT = "TRIP_COUNT";

  @Override
  public String name() {
    return "DAILY_REVENUE_ML_DATASET";
  }

  @Override
  public String outputPath() {
    return "data/YellowTaxiTripRecord/gold/aggregates/DAILY_REVENUE_ML_DATASET.parquet";
  }

  @Override
  public Boolean isNeedToSaved() {
    return false;
  }

  @Override
  public Dataset<Row> aggregate(Dataset<Row> silver) {

    var engagedAt = col(ColumnName.METER_ENGAGED_AT.canonical());

    // =====================================================
    // 1️⃣ DAILY AGGREGATION
    // =====================================================
    Dataset<Row> daily =
        silver
            .withColumn("TRIP_DATE", to_date(engagedAt))
            .groupBy(col("TRIP_DATE"))
            .agg(
                sum("TOTAL_CHARGE_AMOUNT_USD").alias("DAILY_REVENUE_USD"),
                count(lit(1)).alias("TRIP_COUNT"))
            .orderBy(col("TRIP_DATE"));

    // =====================================================
    // 2️⃣ CALENDAR FEATURES
    // =====================================================
    daily =
        daily
            .withColumn("DAY_OF_WEEK", dayofweek(col("TRIP_DATE")))
            .withColumn("IS_WEEKEND", col("DAY_OF_WEEK").isin(1, 7).cast("int"));

    // =====================================================
    // 3️⃣ WINDOWS
    // =====================================================
    WindowSpec orderWindow = Window.orderBy(col("TRIP_DATE"));

    WindowSpec rolling7 = orderWindow.rowsBetween(-7, -1);

    // =====================================================
    // 4️⃣ LAGS
    // =====================================================
    daily =
        daily
            .withColumn("REV_LAG_1", lag(col("DAILY_REVENUE_USD"), 1).over(orderWindow))
            .withColumn("REV_LAG_7", lag(col("DAILY_REVENUE_USD"), 7).over(orderWindow));

    // =====================================================
    // 5️⃣ DELTAS
    // =====================================================
    daily =
        daily
            .withColumn("DELTA_1", col("DAILY_REVENUE_USD").minus(col("REV_LAG_1")))
            .withColumn(
                "DELTA_PCT_1",
                when(col("REV_LAG_1").isNotNull(), col("DELTA_1").divide(col("REV_LAG_1")))
                    .otherwise(lit(0)));

    // =====================================================
    // 6️⃣ ROLLING STATS (NO LEAKAGE)
    // =====================================================
    daily =
        daily
            .withColumn("ROLLING_MEAN_7", avg(col("DAILY_REVENUE_USD")).over(rolling7))
            .withColumn("ROLLING_STD_7", stddev(col("DAILY_REVENUE_USD")).over(rolling7));

    // =====================================================
    // 7️⃣ BINARY SIGNALS
    // =====================================================
    daily =
        daily
            .withColumn("REVENUE_DROP_10P", col("DELTA_PCT_1").lt(-0.10).cast("int"))
            .withColumn(
                "BELOW_ROLLING_MEAN",
                col("DAILY_REVENUE_USD").lt(col("ROLLING_MEAN_7")).cast("int"));

    // =====================================================
    // 8️⃣ GLOBAL VOLATILITY THRESHOLD (CORRECT WAY)
    // =====================================================
    Double volP75 =
        daily
            .filter(col("ROLLING_STD_7").isNotNull())
            .selectExpr("percentile_approx(ROLLING_STD_7, 0.75)")
            .first()
            .getDouble(0);

    daily = daily.withColumn("HIGH_VOLATILITY", col("ROLLING_STD_7").gt(lit(volP75)).cast("int"));

    // =====================================================
    // 9️⃣ FINAL CLEANUP (ML READY)
    // =====================================================
    daily =
        daily
            .filter(col("REV_LAG_7").isNotNull()) // remove cold start
            .na()
            .fill(0);

    // ───────────── Save
    daily.write().mode("overwrite").partitionBy(TRIP_DATE).parquet(outputPath());

    return daily;
  }
}
