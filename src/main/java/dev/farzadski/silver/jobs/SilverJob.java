package dev.farzadski.silver.jobs;

import dev.farzadski.core.jobs.IJob;
import dev.farzadski.core.rules.RuleEngine;
import dev.farzadski.shared.profiling.FrequencyTablePrinter;
import dev.farzadski.silver.enums.ColumnName;
import dev.farzadski.silver.rules.*;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SilverJob implements IJob {
  private final SparkSession spark;

  public SilverJob(SparkSession spark) {
    this.spark = spark;
  }

  @Override
  public String name() {
    return "SILVER_TAXI_STANDARDIZATION_JOB";
  }

  @Override
  public void run() {
    Dataset<Row> trips = spark.read().parquet("data/YellowTaxiTripRecord/bronze/bronze.parquet");
    var rawRuleEngine =
        new RuleEngine<>(
            List.of(
                new FareRateCodeRule(),
                new PassengerCountRule(),
                new StoredBeforeTransmissionFlagRule(),
                new StateCongestionSurchargeAmountUsdRule(),
                new AirportPickupFeeAmountUsdRule(),
                new TripDurationMinutesRule()));

    trips = rawRuleEngine.apply(trips);

    System.out.println(trips.count());
    trips.printSchema();
    FrequencyTablePrinter.print(trips, ColumnName.FARE_RATE_CODE.canonical());
    FrequencyTablePrinter.print(trips, ColumnName.PASSENGER_COUNT.canonical());
    FrequencyTablePrinter.print(trips, ColumnName.STORED_BEFORE_TRANSMISSION_FLAG.canonical());
    FrequencyTablePrinter.print(
        trips, ColumnName.IS_STORED_BEFORE_TRANSMISSION_FLAG_MISSING.canonical());
    FrequencyTablePrinter.print(
        trips, ColumnName.IS_STATE_CONGESTION_SURCHARGE_AMOUNT_USD_MISSING.canonical());
    FrequencyTablePrinter.print(
        trips, ColumnName.IS_AIRPORT_PICKUP_FEE_AMOUNT_USD_MISSING.canonical());
    FrequencyTablePrinter.print(trips, ColumnName.TRIP_DURATION_IN_MINUTES.canonical());

    String silverPath = "data/YellowTaxiTripRecord/silver/silver.parquet";
    trips.write().mode("overwrite").option("compression", "snappy").parquet(silverPath);
  }
}
