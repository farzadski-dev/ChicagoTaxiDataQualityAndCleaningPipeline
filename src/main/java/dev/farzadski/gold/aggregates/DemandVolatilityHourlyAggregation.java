package dev.farzadski.gold.aggregates;

import static org.apache.spark.sql.functions.col;

import dev.farzadski.core.aggregate.IAggregation;
import dev.farzadski.gold.enums.ColumnName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public final class DemandVolatilityHourlyAggregation implements IAggregation {

  private static final String TRIP_DATE = "TRIP_DATE";
  private static final String TRIP_HOUR = "TRIP_HOUR";
  private static final String TRIPS = "TRIPS";

  private static final String AVERAGE_TRIPS = "AVERAGE_TRIPS";
  private static final String TRIP_DEMAND_VARIABILITY = "TRIP_DEMAND_VARIABILITY";
  private static final String DEMAND_VOLATILITY_INDEX = "DEMAND_VOLATILITY_INDEX";
  private static final String VOLATILITY_LEVEL = "VOLATILITY_LEVEL";

  @Override
  public String name() {
    return "DEMAND_VOLATILITY_HOURLY";
  }

  @Override
  public String outputPath() {
    return "data/YellowTaxiTripRecord/gold/aggregates/DEMAND_VOLATILITY_HOURLY.csv";
  }

  @Override
  public Boolean isNeedToSaved() {
    return false;
  }

  @Override
  public Dataset<Row> aggregate(Dataset<Row> silver) {

    // 1️⃣ Hourly trip counts per day
    Dataset<Row> hourlyTripsByDate =
        silver
            .withColumn(TRIP_DATE, functions.to_date(col(ColumnName.METER_ENGAGED_AT.canonical())))
            .withColumn(TRIP_HOUR, functions.hour(col(ColumnName.METER_ENGAGED_AT.canonical())))
            .groupBy(TRIP_DATE, TRIP_HOUR)
            .agg(functions.count("*").alias(TRIPS));

    // 2️⃣ Volatility metrics per hour
    return hourlyTripsByDate
        .groupBy(TRIP_HOUR)
        .agg(
            functions.avg(TRIPS).alias(AVERAGE_TRIPS),
            functions.stddev(TRIPS).alias(TRIP_DEMAND_VARIABILITY))
        // 3️⃣ Normalized volatility index (CV)
        .withColumn(
            DEMAND_VOLATILITY_INDEX, col(TRIP_DEMAND_VARIABILITY).divide(col(AVERAGE_TRIPS)))
        // 4️⃣ Business-friendly volatility buckets
        .withColumn(
            VOLATILITY_LEVEL,
            functions
                .when(col(DEMAND_VOLATILITY_INDEX).gt(0.4), "HIGH")
                .when(col(DEMAND_VOLATILITY_INDEX).gt(0.2), "MEDIUM")
                .otherwise("LOW"));
  }
}
