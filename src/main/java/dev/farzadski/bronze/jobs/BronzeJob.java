package dev.farzadski.bronze.jobs;

import dev.farzadski.bronze.enums.ColumnName;
import dev.farzadski.bronze.rules.RenameColumnRule;
import dev.farzadski.core.jobs.IJob;
import dev.farzadski.core.rules.RuleEngine;
import dev.farzadski.shared.profiling.FrequencyTablePrinter;
import dev.farzadski.shared.profiling.MissingValueProfilePrinter;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class BronzeJob implements IJob {
  private final SparkSession spark;

  public BronzeJob(SparkSession spark) {
    this.spark = spark;
  }

  @Override
  public String name() {
    return "BRONZE_TAXI_STANDARDIZATION_JOB";
  }

  @Override
  public void run() {

    Dataset<Row> trips = spark.read().parquet("data/YellowTaxiTripRecord/raw/");
    var rawRuleEngine = new RuleEngine<>(List.of(new RenameColumnRule()));

    trips = rawRuleEngine.apply(trips);

    System.out.println(trips.count());

    FrequencyTablePrinter.print(trips, ColumnName.PASSENGER_COUNT.canonical());
    FrequencyTablePrinter.print(trips, ColumnName.TRIP_DISTANCE_MILES.canonical());

    MissingValueProfilePrinter.print(trips);
    List<String> json = trips.limit(1).toJSON().collectAsList();
    String bronzePath = "data/YellowTaxiTripRecord/bronze/bronze.parquet";
    trips.write().mode("overwrite").parquet(bronzePath);
    System.out.println(json);
  }
}
