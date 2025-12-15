package dev.farzadski.gold.aggregates;

import static org.apache.spark.sql.functions.col;

import dev.farzadski.core.aggregate.IAggregation;
import dev.farzadski.gold.enums.ColumnName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public final class DemandVolatilityDailyAggregation implements IAggregation {

  private static final String TRIP_DATE = "TRIP_DATE";
  private static final String TRIPS = "TRIPS";

  private static final String AVERAGE_TRIPS = "AVERAGE_TRIPS";
  private static final String TRIP_DEMAND_VARIABILITY = "TRIP_DEMAND_VARIABILITY";
  private static final String DEMAND_VOLATILITY_INDEX = "DEMAND_VOLATILITY_INDEX";
  private static final String VOLATILITY_LEVEL = "VOLATILITY_LEVEL";

  @Override
  public String name() {
    return "DEMAND_VOLATILITY_DAILY";
  }

  @Override
  public String outputPath() {
    return "data/YellowTaxiTripRecord/gold/aggregates/DEMAND_VOLATILITY_DAILY.csv";
  }

  @Override
  public Boolean isNeedToSaved() {
    return false;
  }

  @Override
  public Dataset<Row> aggregate(Dataset<Row> silver) {

    // 1️⃣ Daily trip counts
    Dataset<Row> dailyTrips =
        silver
            .withColumn(TRIP_DATE, functions.to_date(col(ColumnName.METER_ENGAGED_AT.canonical())))
            .groupBy(TRIP_DATE)
            .agg(functions.count("*").alias(TRIPS));

    // 2️⃣ Volatility metrics across days
    return dailyTrips
        .agg(
            functions.avg(TRIPS).alias(AVERAGE_TRIPS),
            functions.stddev(TRIPS).alias(TRIP_DEMAND_VARIABILITY))
        // 3️⃣ Normalized volatility (Coefficient of Variation)
        .withColumn(
            DEMAND_VOLATILITY_INDEX, col(TRIP_DEMAND_VARIABILITY).divide(col(AVERAGE_TRIPS)))
        // 4️⃣ Business-level interpretation
        .withColumn(
            VOLATILITY_LEVEL,
            functions
                .when(col(DEMAND_VOLATILITY_INDEX).gt(0.4), "HIGH")
                .when(col(DEMAND_VOLATILITY_INDEX).gt(0.2), "MEDIUM")
                .otherwise("LOW"));
  }
}
