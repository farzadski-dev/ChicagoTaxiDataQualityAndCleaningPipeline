package dev.farzadski.gold.aggregates;

import dev.farzadski.core.aggregate.IAggregation;
import dev.farzadski.gold.enums.ColumnName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class TripFactHourlyAggregation implements IAggregation {
  private static final String SAT = "Sat";
  private static final String SUN = "Sun";
  private static final String TRIP_DATE = "TRIP_DATE";
  private static final String TRIP_HOUR = "TRIP_HOUR";
  private static final String DAY_OF_WEEK = "DAY_OF_WEEK";
  private static final String IS_WEEKEND = "IS_WEEKEND";
  private static final String TOTAL_TRIPS = "TOTAL_TRIPS";
  private static final String TOTAL_REVENUE_USD = "TOTAL_REVENUE_USD";
  private static final String AVERAGE_FARE_USD = "AVERAGE_FARE_USD";
  private static final String AVERAGE_DISTANCE_MILES = "AVERAGE_DISTANCE_MILES";
  private static final String AVERAGE_DURATION_MINUTES = "AVERAGE_DURATION_MINUTES";
  private static final String AVERAGE_TIP_USD = "AVERAGE_TIP_USD";
  private static final String AIRPORT_TRIP_RATIO = "AIRPORT_TRIP_RATIO";

  @Override
  public String name() {
    return "TRIP_FACT_HOURLY";
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
        .withColumn(
            DAY_OF_WEEK,
            functions.date_format(functions.col(ColumnName.METER_ENGAGED_AT.canonical()), "E"))
        .withColumn(
            IS_WEEKEND, functions.when(functions.col(DAY_OF_WEEK).isin(SAT, SUN), 1).otherwise(0))
        .groupBy(TRIP_DATE, TRIP_HOUR, DAY_OF_WEEK, IS_WEEKEND)
        .agg(
            functions.count("*").alias(TOTAL_TRIPS),
            functions.sum(ColumnName.TOTAL_CHARGE_AMOUNT_USD.canonical()).alias(TOTAL_REVENUE_USD),
            functions.avg(ColumnName.TOTAL_CHARGE_AMOUNT_USD.canonical()).alias(AVERAGE_FARE_USD),
            functions.avg(ColumnName.TRIP_DISTANCE_MILES.canonical()).alias(AVERAGE_DISTANCE_MILES),
            functions
                .avg(ColumnName.TRIP_DURATION_IN_MINUTES.canonical())
                .alias(AVERAGE_DURATION_MINUTES),
            functions.avg(ColumnName.TIP_AMOUNT_USD.canonical()).alias(AVERAGE_TIP_USD),
            functions
                .sum(
                    functions
                        .when(
                            functions
                                .col(ColumnName.AIRPORT_PICKUP_FEE_AMOUNT_USD.canonical())
                                .gt(0),
                            1)
                        .otherwise(0))
                .divide(functions.count("*"))
                .alias(AIRPORT_TRIP_RATIO));
  }

  @Override
  public String outputPath() {
    return "data/YellowTaxiTripRecord/gold/aggregates/TRIP_FACT_HOURLY.csv";
  }
}
