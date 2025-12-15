package dev.farzadski.gold.aggregates;

import dev.farzadski.core.aggregate.IAggregation;
import dev.farzadski.gold.enums.ColumnName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class ZoneDemandHourlyAggregation implements IAggregation {
  private static final String TRIP_DATE = "TRIP_DATE";
  private static final String TRIP_HOUR = "TRIP_HOUR";
  private static final String TOTAL_TRIPS = "TOTAL_TRIPS";
  private static final String TOTAL_REVENUE_USD = "TOTAL_REVENUE_USD";
  private static final String AVERAGE_DURATION_MINUTES = "AVERAGE_DURATION_MINUTES";

  @Override
  public String name() {
    return "ZONE_DEMAND_HOURLY";
  }

  @Override
  public Boolean isNeedToSaved() {
    return false;
  }

  @Override
  public Dataset<Row> aggregate(Dataset<Row> silver) {

    return silver
        .withColumn(
            TRIP_DATE, functions.to_date(functions.col(ColumnName.METER_ENGAGED_AT.canonical())))
        .withColumn(
            TRIP_HOUR, functions.hour(functions.col(ColumnName.METER_ENGAGED_AT.canonical())))
        .groupBy(TRIP_DATE, TRIP_HOUR, ColumnName.PICKUP_TAXI_ZONE_ID.canonical())
        .agg(
            functions.count("*").alias(TOTAL_TRIPS),
            functions.sum(ColumnName.TOTAL_CHARGE_AMOUNT_USD.canonical()).alias(TOTAL_REVENUE_USD),
            functions
                .avg(ColumnName.TRIP_DURATION_IN_MINUTES.canonical())
                .alias(AVERAGE_DURATION_MINUTES));
  }

  @Override
  public String outputPath() {
    return "data/YellowTaxiTripRecord/gold/aggregates/ZONE_DEMAND_HOURLY.csv";
  }
}
