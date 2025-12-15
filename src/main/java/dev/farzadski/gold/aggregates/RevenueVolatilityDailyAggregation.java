package dev.farzadski.gold.aggregates;

import static org.apache.spark.sql.functions.col;

import dev.farzadski.core.aggregate.IAggregation;
import dev.farzadski.gold.enums.ColumnName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public final class RevenueVolatilityDailyAggregation implements IAggregation {

  private static final String TRIP_DATE = "TRIP_DATE";
  private static final String REVENUE = "REVENUE";

  private static final String AVERAGE_REVENUE = "AVERAGE_REVENUE_USD";
  private static final String REVENUE_VARIABILITY = "REVENUE_VARIABILITY";
  private static final String REVENUE_VOLATILITY_INDEX = "REVENUE_VOLATILITY_INDEX";
  private static final String VOLATILITY_LEVEL = "VOLATILITY_LEVEL";

  @Override
  public String name() {
    return "REVENUE_VOLATILITY_DAILY";
  }

  @Override
  public String outputPath() {
    return "data/YellowTaxiTripRecord/gold/aggregates/REVENUE_VOLATILITY_DAILY.csv";
  }

  @Override
  public Boolean isNeedToSaved() {
    return false;
  }

  @Override
  public Dataset<Row> aggregate(Dataset<Row> silver) {

    // 1️⃣ Total revenue per day
    Dataset<Row> dailyRevenue =
        silver
            .withColumn(TRIP_DATE, functions.to_date(col(ColumnName.METER_ENGAGED_AT.canonical())))
            .groupBy(TRIP_DATE)
            .agg(functions.sum(col(ColumnName.TOTAL_CHARGE_AMOUNT_USD.canonical())).alias(REVENUE));

    // 2️⃣ Revenue volatility across days
    return dailyRevenue
        .agg(
            functions.avg(REVENUE).alias(AVERAGE_REVENUE),
            functions.stddev(REVENUE).alias(REVENUE_VARIABILITY))
        // 3️⃣ Normalized volatility index
        .withColumn(REVENUE_VOLATILITY_INDEX, col(REVENUE_VARIABILITY).divide(col(AVERAGE_REVENUE)))
        // 4️⃣ Business-friendly bucket
        .withColumn(
            VOLATILITY_LEVEL,
            functions
                .when(col(REVENUE_VOLATILITY_INDEX).gt(0.4), "HIGH")
                .when(col(REVENUE_VOLATILITY_INDEX).gt(0.2), "MEDIUM")
                .otherwise("LOW"));
  }
}
